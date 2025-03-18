# Mocks

The RON files are for mocking remote service responses in unit tests.

There is a bash script called `create_tx.sh` which can create the serialized `tx` field for the
`MockClient` struct. The TxId list can be patched to create other mocks. This script creates a file
named `mock_tx.ron` in the current working directory with contents that can be copy-pasted directly
into the fixture RON file with minimal changes, mostly just indentation.

The `blocks` field can be ignored for testing purposes. It's only used by the "generic wallet" tests
to ensure the memoization works correctly. Nevertheless, there is a `create_blocks.sh` script to
create this field if you wish to play with it.
