# protomap-test-golang

A synthetic code base of the fictional company Acme for testing ProtoMap: 480 gRPC services with 3606 methods in 24
domains, and 13286 protobuf messages with 57780 fields, many nested several levels deep.

It was generated, with the eight other `protomap-test-*` repositories, from one catalog of Acme's APIs. The repositories
own disjoint domains and call each other's services, so each holds copies of the protobuf files it needs from the
others, byte for byte as their owners have them.

## Layout

The Go code base is one module and one deployable, a monolith that serves every service of its
domains on one gRPC server.

- `proto/` holds the protobuf files of the domains it owns, the shared `acme/common/v1` files, and copies of the files
  of other repositories' services it calls. `gen/` holds what `protoc-gen-go` and `protoc-gen-go-grpc` make of them.
- `cmd/monolith` registers every domain behind an interceptor that checks each call's session with the identity
  service, and starts the domains' event workers. The other commands are batch jobs and the operators' CLI.
- The monolith takes its PostgreSQL database from `DATABASE_URL`, and signs the calls it makes, its session checks
  included, with `ACME_SERVICE_TOKEN`, which its replicas share. The other commands call with the bearer token in
  `ACME_TOKEN`, for the tenant of their `-tenant` flag.
- `internal/platform` is the platform team's code: gRPC helpers, interceptors, the client registry, the event bus,
  mappers, caches, and the middleware pipeline. `internal/<domain>` holds a domain's servers, one file per service, and
  its store, ports, client wrappers, request builders, and workers.

Build it with `GOFLAGS=-mod=mod go build ./...`; it needs Go 1.25.

The domains are `storefront`, `partnerapi`, `identity`, `search`, `customers`, `catalog`, `pricing`, `promotions`,
`cart`, `checkout`, `orders`, `payments`, `fraud`, `inventory`, `warehousing`, `fulfillment`, `shipping`, `returns`,
`subscriptions`, `loyalty`, `reviews`, `merchandising`, `sellers`, `giftcards`. It calls services of `accounting`,
`admissions`, `analytics`, `anomalies`, `appointments`, `assets`, `audiences`, `audit`, `authorization`, `banking`,
`benefits`, `billing`, `boxoffice`, `broadcasts`, `budgeting`, `campaigns`, `careplans`, `certificates`, `chat`,
`collections`, `community`, `compliance`, `concessions`, `configuration`, `content`, `contracts`, `corporateevents`,
`datahub`, `datasets`, `deliveries`, `devices`, `dispatch`, `documents`, `driverapp`, `drivers`, `edgegateway`,
`employeeportal`, `employees`, `expenses`, `experiments`, `fanportal`, `featureflags`, `featurestore`, `financeportal`,
`fleet`, `forecasting`, `forms`, `fuel`, `insurance`, `invoicing`, `itsm`, `keymanagement`, `labeling`, `learning`,
`ledger`, `lending`, `licensing`, `lineage`, `localization`, `maintenance`, `media`, `messaging`, `metrics`,
`modelregistry`, `notifications`, `onboarding`, `patientapp`, `patients`, `payroll`, `performance`, `performers`,
`personalization`, `pipelines`, `prescriptions`, `procurement`, `ratelimiting`, `recommendations`, `recruiting`, `risk`,
`routing`, `scheduling`, `seating`, `secrets`, `shows`, `sponsorships`, `suppliers`, `surveys`, `telehealth`,
`telematics`, `ticketing`, `timekeeping`, `tours`, `travel`, `treasury`, `venues`, `webbff`, `webhooks`, `workplaces`,
`yards` too.

## Oracle

`expected.json` lists what scanning the repository must find: every handler, every call a handler or a program makes,
and every field-level flow from where a handler has data to where a call or its response takes it, each tagged with the
shape of the code that makes it. `crates/graph/tests/fixture_coverage.rs` in `protomap-source` checks a scan against it.

The code mixes in, on purpose, what makes static analysis of real code bases hard. Flows by pattern:

| Pattern | Flows |
|---|---|
| `bidi_call` | 26 |
| `channel_each` | 291 |
| `client_stream_call` | 41 |
| `closure_factory` | 775 |
| `deep_pipeline` | 597 |
| `deprecated_invoke` | 586 |
| `direct` | 4073 |
| `dispatch_table` | 640 |
| `echo` | 5149 |
| `embedded_wrapper` | 1193 |
| `errgroup_each` | 463 |
| `event_bus` | 322 |
| `func_field` | 934 |
| `functional_options` | 944 |
| `generic_call` | 1865 |
| `generic_factory` | 860 |
| `goroutine_select` | 1088 |
| `helper_builder` | 1820 |
| `invoke_concat` | 816 |
| `invoke_fullmethod` | 1109 |
| `invoke_sprintf` | 778 |
| `iterator_each` | 327 |
| `lazy_client` | 772 |
| `loop_each` | 513 |
| `method_expression` | 952 |
| `method_value_map` | 601 |
| `package_var` | 1018 |
| `port_adapter` | 1336 |
| `program_job` | 38 |
| `program_stream` | 5 |
| `registry_assert` | 814 |
| `registry_lookup` | 855 |
| `retry_closure` | 1721 |
| `saga` | 912 |
| `server_stream_call` | 1148 |
| `server_stream_handler` | 107 |
| `stepwise` | 2258 |

Flows whose values pass through something on their way, by what:

| Transform | Flows |
|---|---|
| `any_pack` | 445 |
| `any_unpack` | 389 |
| `clone` | 471 |
| `context_value` | 596 |
| `cross_handler_cache` | 12 |
| `generic_cache` | 252 |
| `json_bytes` | 163 |
| `json_roundtrip` | 274 |
| `map_copy` | 485 |
| `oneof_switch` | 380 |
| `protoreflect_get` | 157 |
| `recursion` | 276 |
| `reflect_copy` | 134 |

Calls by pattern:

| Pattern | Calls |
|---|---|
| `bidi_call` | 9 |
| `channel_each` | 83 |
| `client_stream_call` | 12 |
| `closure_factory` | 156 |
| `deep_pipeline` | 122 |
| `deprecated_invoke` | 116 |
| `direct` | 832 |
| `dispatch_table` | 133 |
| `embedded_wrapper` | 247 |
| `errgroup_each` | 131 |
| `event_bus` | 112 |
| `func_field` | 182 |
| `functional_options` | 193 |
| `generic_call` | 382 |
| `generic_factory` | 173 |
| `goroutine_select` | 225 |
| `helper_builder` | 391 |
| `invoke_concat` | 158 |
| `invoke_fullmethod` | 226 |
| `invoke_sprintf` | 157 |
| `iterator_each` | 92 |
| `lazy_client` | 160 |
| `loop_each` | 147 |
| `method_expression` | 195 |
| `method_value_map` | 119 |
| `package_var` | 209 |
| `port_adapter` | 272 |
| `program_cli` | 6 |
| `program_job` | 20 |
| `program_stream` | 4 |
| `registry_assert` | 166 |
| `registry_lookup` | 171 |
| `retry_closure` | 347 |
| `saga` | 178 |
| `server_stream_call` | 332 |
| `stepwise` | 465 |
