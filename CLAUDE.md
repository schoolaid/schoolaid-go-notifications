# CLAUDE.md

Go library that publishes SchoolAid notification messages to Kafka with the same schemas, topic
names and headers as `schoolaid-admin`'s Laravel `KafkaChannel`. `README.md` covers usage.

## Commands

- Go 1.22 (`go.mod`). `go vet ./...` and `go test ./...` (both pass on `main`).
- There is no CI: run both yourself before tagging a release.

## Wire format follows schoolaid-admin

The Laravel `KafkaChannel` is the source of truth for the message shape. Change it there first,
then mirror it here and in the contract tests (`contract_test.go`). The consumer's schemas are in
the `notifications` repo, under `docs/contracts/`.

## Releases

A release is a semver tag (`git tag vX.Y.Z && git push origin vX.Y.Z`; latest is v0.5.0).
Consumers pin their own versions (`schoolaid-external-api` v0.3.0, `schoolaid-positions-api`
v0.4.0 today), so a new tag reaches no service until that service bumps its `go.mod`.
