# Fixtures

Store static test inputs and expected outputs here. Go package discovery skips
`testdata` directories; executable tests stay beside their domain code.

`lint/` preserves the original lint diagnostics and isolated data-loss
reproduction from the initial audit. Their paths, package name, and test-helper
signatures describe the project before the fixes and layout migration. Current
regression tests live in `internal/workspace`.
