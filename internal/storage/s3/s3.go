// Package s3 stores immutable blocks through the S3 API, including Cloudflare R2.
package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"example.com/dfs/internal/metadata"
	"example.com/dfs/internal/storage"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

// Config separates the account endpoint from the bucket. Prefix is a required
// dedicated volume namespace; credentials never form part of its identity.
type Config struct {
	Endpoint        string
	Bucket          string
	Region          string
	Prefix          string
	AccessKeyID     string
	SecretAccessKey string
	HTTPClient      *http.Client
}

type Store struct {
	client *awss3.Client
	bucket string
	prefix string
}

var _ storage.ObjectStore = (*Store)(nil)

// New constructs a client without creating buckets or making network requests.
func New(config Config) (*Store, error) {
	endpoint, err := url.Parse(config.Endpoint)
	if err != nil || endpoint.Host == "" || (endpoint.Scheme != "https" && endpoint.Scheme != "http") || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" || (endpoint.Path != "" && endpoint.Path != "/") {
		return nil, errors.New("S3 endpoint must be an HTTP(S) origin without a bucket path or credentials")
	}
	if config.Bucket == "" || strings.ContainsAny(config.Bucket, "/\\ \t\r\n") || config.AccessKeyID == "" || config.SecretAccessKey == "" {
		return nil, errors.New("S3 bucket, access key ID, and secret access key are required")
	}
	if config.Prefix == "" || strings.HasPrefix(config.Prefix, "/") {
		return nil, fs.ErrInvalid
	}
	for _, c := range config.Prefix {
		if c != '/' && c != '-' && c != '_' && (c < '0' || c > '9') && (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') {
			return nil, fs.ErrInvalid
		}
	}
	if strings.Trim(config.Prefix, "/") == "" || strings.Contains(config.Prefix, "//") {
		return nil, fs.ErrInvalid
	}
	if config.Region == "" {
		config.Region = "auto"
	}
	if config.HTTPClient == nil {
		config.HTTPClient = &http.Client{Timeout: 30 * time.Second}
	}
	cfg := aws.Config{
		Region:                     config.Region,
		Credentials:                credentials.NewStaticCredentialsProvider(config.AccessKeyID, config.SecretAccessKey, ""),
		HTTPClient:                 config.HTTPClient,
		RetryMaxAttempts:           3,
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired,
	}
	client := awss3.NewFromConfig(cfg, func(o *awss3.Options) {
		o.BaseEndpoint = aws.String(strings.TrimSuffix(config.Endpoint, "/"))
		o.UsePathStyle = true
	})
	return &Store{client: client, bucket: config.Bucket, prefix: strings.TrimSuffix(config.Prefix, "/") + "/blocks/"}, nil
}

func (s *Store) objectKey(key string) (string, error) {
	if !metadata.ValidContentKey(key) {
		return "", fs.ErrInvalid
	}
	return s.prefix + key, nil
}

// Check verifies access to the existing bucket; provisioning is external.
func (s *Store) Check(ctx context.Context) error {
	_, err := s.client.HeadBucket(ctx, &awss3.HeadBucketInput{Bucket: aws.String(s.bucket)})
	return translate(err)
}

func (s *Store) Put(ctx context.Context, key string, data []byte) error {
	object, err := s.objectKey(key)
	if err != nil {
		return err
	}
	_, err = s.client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(object), Body: bytes.NewReader(data),
		ContentLength: aws.Int64(int64(len(data))), IfNoneMatch: aws.String("*"),
	})
	return translate(err)
}

func (s *Store) Get(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	object, err := s.objectKey(key)
	if err != nil {
		return nil, err
	}
	if offset < 0 || length <= 0 || length-1 > math.MaxInt64-offset {
		return nil, fs.ErrInvalid
	}
	end := offset + length - 1
	output, err := s.client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(object), Range: aws.String(fmt.Sprintf("bytes=%d-%d", offset, end)),
	})
	if err != nil {
		return nil, translate(err)
	}
	want := fmt.Sprintf("bytes %d-%d/", offset, end)
	contentRange := aws.ToString(output.ContentRange)
	total, parseErr := strconv.ParseInt(strings.TrimPrefix(contentRange, want), 10, 64)
	if !strings.HasPrefix(contentRange, want) || parseErr != nil || total <= end || output.ContentLength == nil || *output.ContentLength != length {
		return nil, errors.Join(errors.New("S3 returned an invalid byte range"), output.Body.Close())
	}
	return output.Body, nil
}

func (s *Store) Head(ctx context.Context, key string) (int64, error) {
	object, err := s.objectKey(key)
	if err != nil {
		return 0, err
	}
	output, err := s.client.HeadObject(ctx, &awss3.HeadObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(object)})
	if err != nil {
		return 0, translate(err)
	}
	if output.ContentLength == nil || *output.ContentLength < 0 {
		return 0, errors.New("S3 returned an invalid object length")
	}
	return *output.ContentLength, nil
}

func (s *Store) Delete(ctx context.Context, key string) error {
	object, err := s.objectKey(key)
	if err != nil {
		return err
	}
	_, err = s.client.DeleteObject(ctx, &awss3.DeleteObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(object)})
	if errors.Is(translate(err), fs.ErrNotExist) {
		return nil
	}
	return translate(err)
}

func (s *Store) List(ctx context.Context) ([]string, error) {
	input := &awss3.ListObjectsV2Input{Bucket: aws.String(s.bucket), Prefix: aws.String(s.prefix)}
	var keys []string
	seen := make(map[string]bool)
	for {
		page, err := s.client.ListObjectsV2(ctx, input)
		if err != nil {
			return nil, translate(err)
		}
		for _, object := range page.Contents {
			key := strings.TrimPrefix(aws.ToString(object.Key), s.prefix)
			if !strings.HasPrefix(aws.ToString(object.Key), s.prefix) || !metadata.ValidContentKey(key) {
				return nil, errors.New("S3 listing contains an object outside the block namespace")
			}
			keys = append(keys, key)
		}
		if !aws.ToBool(page.IsTruncated) {
			break
		}
		token := aws.ToString(page.NextContinuationToken)
		if token == "" || seen[token] {
			return nil, errors.New("S3 listing has an invalid continuation token")
		}
		seen[token] = true
		input.ContinuationToken = aws.String(token)
	}
	sort.Strings(keys)
	return keys, nil
}

func translate(err error) error {
	var api smithy.APIError
	if !errors.As(err, &api) {
		return err
	}
	switch api.ErrorCode() {
	case "NoSuchKey", "NoSuchBucket", "NotFound", "404":
		return errors.Join(fs.ErrNotExist, err)
	case "AccessDenied", "Forbidden", "InvalidAccessKeyId", "SignatureDoesNotMatch", "403":
		return errors.Join(fs.ErrPermission, err)
	case "PreconditionFailed", "412":
		return errors.Join(fs.ErrExist, err)
	default:
		return err
	}
}
