# Changelog

This file documents all notable changes to the Ballerina Azure Files package. The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- Remove conflicting jar file warnings during consumer builds by aligning the `data.jsondata` and `data.xmldata` dependency versions with the distribution and marking `slf4j-api` as test-only

## [1.0.2] - 2026-09-08

### Fixed

- Add metadata files

## [1.0.1] - 2026-08-21

### Added

- `onFileCsv` accepts the string row forms alongside the record forms: `string[][]` and `stream<string[], error?>`. The string forms keep every row of the file, the header row included
- Named type aliases are accepted for a listener handler's content, `FileInfo`, and `Caller` parameters

### Fixed

- Typed service errors raised while a content stream is read keep their Azure status and error code instead of collapsing to the generic client error
- A ranged `downloadToFile` no longer drops the range's last byte
- The CSV row stream and the content streams close their sources when parsing or reading fails
- Listener diagnostics reach the Ballerina log (they were silently discarded), a handler panic triggers the `afterError` consume action, and a stopped listener rejects a restart instead of never polling again
- The compiler plugin's type-reference walks are bounded and null-guarded, so erroneous or in-progress sources cannot hang or crash the analysis, or the IDE's language server running it
- A failure to invoke the `onError` handler is reported as a Ballerina error with its stack trace

### Changed

- `RetryPolicyType.FIXED` is renamed `FIXED_INTERVAL`; the configuration value stays `"fixed"`, and the `FIXED` constant remains available through the shared `LeaseDuration` member, so existing code keeps compiling

## [1.0.0] - 2026-08-19

### Added

- Initial `Client` and `AdminClient` surface: share, directory, file, transfer, copy, and range operations, plus share snapshots, SAS generation, and account service configuration
- Shared key, SAS token, SAS URL, connection string, and Microsoft Entra ID authentication, with configurable retry, proxy, connection-pool, and TLS transport settings
- A polling `Listener` and `Caller` for event-style consumption: the listener watches the path given by the service's attach point (the share root when absent) and dispatches each present file to a content handler (`onFile`, or the typed `onFileText`, `onFileJson`, `onFileXml`, and `onFileCsv` variants) by file extension, with optional `@files:ServiceConfig` filters and `@files:FunctionConfig` auto-consume actions (delete or move)
- Typed content binding through the Ballerina data modules, following the shared file-modules databinding contract: matching `UploadContent` and `RetrievableType` unions (`byte[]`, `string`, `json`, `xml`, records, record arrays, and the byte and CSV record stream forms), format resolution from a `fileFormat` override or the path extension, records-only CSV binding, and a `laxDataBinding` option on the listener
- An optional `onError` service handler notified of every listener-side failure: poll failures and content-read failures as mapped typed errors, and a typed handler's content-binding failure as a `ContentBindingError` carrying the failing file's path. With `onError` declared, the binding-failed file's fate follows `onError`'s own `@files:FunctionConfig`
- A compiler plugin that validates a listener service at compile time (at least one content handler, each handler's parameter types and `error?` return, the `onError` signature, and no resource functions or unknown remote methods)
- A test suite that runs against an in-process mock of the Azure Files REST service without credentials, and against a live storage account when credentials are configured
- GraalVM native-image support (verified by running the test suite as a native executable)
