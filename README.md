# Single-node file system

A single-process, disk-backed FUSE filesystem in Go. Files and metadata live in
a local backing directory, exposed through a mount that ordinary commands can
use. This is the first milestone toward a distributed filesystem.

```mermaid
flowchart LR
    App[Shell commands and applications] --> FUSE[FUSE mount]
    FUSE --> FS[localfs]
    FS --> Disk[Backing directory]
```

Supports files and directories, offset reads/writes, append, truncate, permissions,
timestamps, rename, deletion, and file/directory `fsync`. Open file descriptors
remain valid after rename or unlink. Files and metadata persist in the backing
filesystem across restarts.

## Run

Requires Go 1.25+ and a FUSE driver:

- macOS: install [macFUSE](https://macfuse.github.io/) and complete its OS setup.
- Linux: enable `/dev/fuse` and install `fusermount` or `fusermount3`.

```sh
mkdir -p mnt
go run ./cmd/singlefs -data ./data ./mnt
```

The mountpoint must be an existing empty directory, separate from the backing
directory. The command stays in the foreground while the filesystem is mounted.

In another terminal:

```sh
mkdir mnt/docs
printf 'hello\n' > mnt/docs/hello.txt
cat mnt/docs/hello.txt
mv mnt/docs/hello.txt mnt/docs/saved.txt
```

Press Ctrl+C in the mount process to unmount, then run the same mount command
again and check persistence:

```sh
cat mnt/docs/saved.txt
```

External `umount ./mnt` also stops the process. Close open files if unmount
reports that the filesystem is busy.

## Layout and checks

```text
cmd/singlefs/      Mount command and argument tests
internal/localfs/  Filesystem operations and FUSE integration
test/              Filesystem behavior and optional mounted smoke tests
```

```sh
go test -race ./...
go vet ./...
# Optional, with golangci-lint installed:
golangci-lint run --no-config ./...
# With a FUSE driver installed:
DFS_MOUNT_TEST=1 go test -race -run '^TestMountedFilesystem$' ./test
```

The regular suite tests callbacks without mounting, including restart persistence,
concurrent append, rename/unlink with open descriptors, and directory pagination.
The opt-in smoke test mounts, writes, unmounts, mounts again, and reads the file.
It is skipped unless `DFS_MOUNT_TEST=1`; callback tests alone do not verify a
working kernel mount.

## Current limits

This is a single-user POSIX-style subset. Links, ownership changes, extended
attributes, file locking, and full POSIX compliance are outside this first version.
One process owns a backing directory at a time; change its contents through the
mount while running. Inode metadata is cached until unmount. Data durability is
provided by the host filesystem through `fsync`; replication comes later.
