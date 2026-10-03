# protomap-test-golang

A synthetic Go code base for testing [ProtoMap](https://protomap.ai) by [HoundDog.ai](https://hounddog.ai). It is part
of the code of Acme, a fictional company.

The code mixes in, on purpose, what makes static analysis of real code bases hard, and `expected.json` is the answer key
of what a scanner should find in it.

Acme's code is spread over multiple repositories, one per programming language. They were generated together from one
catalog of Acme's APIs. Each owns its own domains and calls the others' services, so it holds copies of the protobuf
files it needs from them, byte for byte as their owners have them. Scan them together to follow calls and data from one
repository to another.

| Repository                                                                         | Language   |
|------------------------------------------------------------------------------------|------------|
| [protomap-test-csharp](https://github.com/hounddogai/protomap-test-csharp)         | C#         |
| **protomap-test-golang** (this one)                                                | Go         |
| [protomap-test-java](https://github.com/hounddogai/protomap-test-java)             | Java       |
| [protomap-test-javascript](https://github.com/hounddogai/protomap-test-javascript) | JavaScript |
| [protomap-test-kotlin](https://github.com/hounddogai/protomap-test-kotlin)         | Kotlin     |
| [protomap-test-python](https://github.com/hounddogai/protomap-test-python)         | Python     |
| [protomap-test-rust](https://github.com/hounddogai/protomap-test-rust)             | Rust       |
| [protomap-test-swift](https://github.com/hounddogai/protomap-test-swift)           | Swift      |
| [protomap-test-typescript](https://github.com/hounddogai/protomap-test-typescript) | TypeScript |

## Repository Layout

One Go module and one deployable: a monolith that serves every service of its domains on one gRPC server.

- `proto/`: the protobuf files of the domains it owns, the shared `acme/common/v1` files, and copies of the files of
  other repositories' services it calls.
- `gen/`: what `protoc-gen-go` and `protoc-gen-go-grpc` generate from them.
- `cmd/monolith`: registers every domain behind an interceptor that checks each call's session with the identity
  service, and starts the domains' event workers. The other commands are batch jobs and the operators' CLI.
- `internal/platform`: the platform team's code: gRPC helpers, interceptors, the client registry, the event bus,
  mappers, caches, and the middleware pipeline.
- `internal/<domain>`: a domain's servers, one file per service, and its store, ports, client wrappers, request
  builders, and workers.

## Build and Run

Build with Go 1.25 or newer:

```sh
GOFLAGS=-mod=mod go build ./...
```

The monolith takes its PostgreSQL database from `DATABASE_URL`, and signs the calls it makes, its session checks
included, with `ACME_SERVICE_TOKEN`, which its replicas share. The other commands call with the bearer token in
`ACME_TOKEN`, for the tenant of their `-tenant` flag.
