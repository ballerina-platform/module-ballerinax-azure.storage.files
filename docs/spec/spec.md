# Specification: Ballerina Azure Files Library

_Owners_: @YasanPunch \
_Reviewers_: @niveathika \
_Created_: 2026/07/13 \
_Updated_: 2026/09/08 \
_Edition_: Swan Lake

## Introduction

This specification describes the Azure Files connector library for the Ballerina programming language, enabling applications to manage Microsoft Azure file shares and the directories and files within them. The library definition has progressed over time and may undergo further refinement. Previous versions are accessible via their corresponding GitHub tags.

For feedback or suggestions regarding this library, please open a discussion through a "GitHub issue" or participate in the "Discord server". Community input drives potential updates to both specification and implementation. Accepted proposals that impact the specification are documented in `/docs/proposals`, with ongoing discussions tagged as `type/proposal` on GitHub.

The official implementation aligns with this specification. Any deviation qualifies as a defect.

## Contents

1. [Overview](#1-overview)
2. [Configuration](#2-configuration)
   * 2.1 [Authentication](#21-authentication)
   * 2.2 [Client Configuration](#22-client-configuration)
   * 2.3 [Retry Configuration](#23-retry-configuration)
   * 2.4 [Transport Configuration](#24-transport-configuration)
3. [AdminClient](#3-adminclient)
   * 3.1 [Initializing the AdminClient](#31-initializing-the-adminclient)
   * 3.2 [Share Management Operations](#32-share-management-operations)
   * 3.3 [Service Configuration Operations](#33-service-configuration-operations)
   * 3.4 [User Delegation Key and Account SAS](#34-user-delegation-key-and-account-sas)
4. [Client](#4-client)
   * 4.1 [Initializing the Client](#41-initializing-the-client)
   * 4.2 [Share Operations](#42-share-operations)
   * 4.3 [Directory Operations](#43-directory-operations)
   * 4.4 [File Operations](#44-file-operations)
   * 4.5 [Transfer Operations](#45-transfer-operations)
   * 4.6 [Copy Operations](#46-copy-operations)
   * 4.7 [Range Operations](#47-range-operations)
   * 4.8 [Share Snapshot Operations](#48-share-snapshot-operations)
   * 4.9 [SAS Generation](#49-sas-generation)
5. [The Listener and Caller](#5-the-listener-and-caller)
   * 5.1 [Initializing the Listener](#51-initializing-the-listener)
   * 5.2 [The Service and the Watched Path](#52-the-service-and-the-watched-path)
   * 5.3 [Content Handlers](#53-content-handlers)
   * 5.4 [Consuming Files](#54-consuming-files)
   * 5.5 [Error Notification](#55-error-notification)
   * 5.6 [Delivery Semantics](#56-delivery-semantics)
   * 5.7 [The Caller](#57-the-caller)
6. [Errors](#6-errors)
7. [Observability](#7-observability)
   * 7.1 [Metrics](#71-metrics)
   * 7.2 [Tags](#72-tags)
   * 7.3 [File Lifecycle Tracing](#73-file-lifecycle-tracing)

## 1. Overview

[Azure Files](https://learn.microsoft.com/en-us/azure/storage/files/storage-files-introduction) is the fully managed file share service of Azure Storage, offering shares accessible over SMB, NFS, and REST. The `ballerinax/azure.storage.files` module provides an idiomatic Ballerina API for the service. It is built on the official Azure SDK for Java (`com.azure:azure-storage-file-share`), which supplies request signing, retry, and chunked transfer underneath the Ballerina surface.

The public surface is four types:

* The `AdminClient` operates at the storage account level. It creates, lists, deletes, and restores shares, manages the account's file service configuration, and mints account level SAS tokens.
* The `Client` is bound to a single share at initialization and carries every operation inside that share: directories, files, transfers, copies, byte ranges, snapshots, and SAS generation.
* The `Listener` polls one watched path on a share and dispatches each present file to the matching content handler of its attached service.
* The `Caller` is passed to each listener handler. It forwards a curated share scoped subset of the `Client`, so a handler can act on the event's file without constructing a separate client.

Directory and file operations take a single slash delimited, share relative path (for example `/reports/2026/q4.pdf`). Where two paths co-occur, they are named `sourcePath` and `destinationPath`, in source first order. Every entry returned by a listing carries its full share relative path, so listing results feed directly into the path taking operations.

Every operation that calls the service is a remote method, invoked with `->`. Methods that make no service call are ordinary methods, invoked with `.`: the `Listener`'s lifecycle methods and the SAS generation methods, which sign tokens locally with the credential the client already holds. The clients hold no releasable resources, so there is no close method; a client that is no longer needed is simply discarded.

## 2. Configuration

### 2.1 Authentication

The authentication configuration is a union in which each member represents exactly one real world credential artifact, the thing the Azure portal, CLI, or infrastructure tooling actually hands the user:

```ballerina
public type AuthConfig SharedKeyConfig|SasConfig|SasUrlConfig|ConnectionStringConfig|EntraIdConfig;
```

Every member has a unique required field or field combination, so both the compiler and `Config.toml` select the right member by structural matching, with no discriminator field. The two Microsoft Entra ID chain records (`DefaultEntraIdConfig` and `ManagedIdentityConfig`), which would otherwise share the same field shape, are the exception: they carry a `kind` discriminator.

###### Example: Selecting an Authentication Mode

```ballerina
// The fields present select the union member:
files:AuthConfig sharedKey = {accountName: "myacct", accountKey: "..."};
files:AuthConfig sasToken = {accountName: "myacct", sasToken: "sv=..."};
files:AuthConfig sasUrl = {sasUrl: "https://myacct.file.core.windows.net/?sv=..."};
files:AuthConfig connectionString = {connectionString: "..."};
files:AuthConfig defaultChain = {kind: "default", accountName: "myacct"};
files:AuthConfig managedIdentity = {kind: "managed-identity", accountName: "myacct"};
files:AuthConfig servicePrincipal = {accountName: "myacct", tenantId: "...", clientId: "...", clientSecret: "..."};
files:AuthConfig certificate = {accountName: "myacct", tenantId: "...", clientId: "...", certificatePath: "/path/cert.pem"};
files:AuthConfig workloadIdentity = {accountName: "myacct", tenantId: "...", clientId: "...", tokenFilePath: "/path/token"};
```

The five modes:

* **Shared key** (`SharedKeyConfig`): authenticates with the storage account name and one of its access keys. The required `accountName` is the signing identity and derives the service URL; the required `accountKey` is a base64 encoded access key. The optional `serviceUrl` overrides the endpoint, defaulting to `https://{accountName}.file.core.windows.net`.
* **SAS token** (`SasConfig`): authenticates with a bare shared access signature token, as issued by `az storage share generate-sas` or the SAS generation methods of this module. Requires `accountName` (which determines the service URL) and `sasToken`.
* **SAS URL** (`SasUrlConfig`): authenticates with a full SAS URL, which carries the service URL and the SAS token in one string, as issued by the Azure portal ("File service SAS URL"). Requires `sasUrl`, including the scheme and the SAS query string.
* **Connection string** (`ConnectionStringConfig`): authenticates with a storage account connection string, which bundles the account name, the credential (an account key or a SAS token), and the service endpoints. Requires `connectionString`.
* **Microsoft Entra ID** (`EntraIdConfig`): itself a union of five records, one per credential kind. `DefaultEntraIdConfig` (`kind = "default"`) tries the environment, a managed identity, and developer sign-ins in turn. `ManagedIdentityConfig` (`kind = "managed-identity"`) authenticates as an Azure managed identity, with an optional `clientId` selecting a user assigned identity. `ClientSecretConfig`, `ClientCertificateConfig`, and `WorkloadIdentityConfig` authenticate as a service principal and are distinguished by their unique required field: `clientSecret`, `certificatePath` (PEM, or PFX when `certificatePassword` is set), or `tokenFilePath` (the federated service account token of a Kubernetes workload). All five require `accountName` and accept an optional `serviceUrl` override; the service principal records also require `tenantId` and `clientId`.

Every auth mode is validated at `init` with local computation and no call to Azure: connection strings are parsed strictly and checked for a file endpoint, and the explicit records get non-empty, base64, and URL scheme checks. A malformed credential surfaces a specific error at `init` rather than an opaque failure at first use.

Azure Files honors OAuth tokens only on requests carrying the backup intent, which the connector sets automatically for Entra ID clients. The intent bypasses file and directory ACLs and requires the identity to hold the `Storage File Data Privileged Reader` or `Storage File Data Privileged Contributor` role. Those roles cover the file and directory data operations; share level and account level management operations (the `AdminClient` surface, and the `Client` operations on the share itself) authorize against the storage account's management role actions instead (`Microsoft.Storage/storageAccounts/fileServices/shares/` read, write, and delete, carried by roles such as `Contributor`), so an identity covering the full surface holds both a privileged data role and a management role.

### 2.2 Client Configuration

Both clients take the same `ClientConfiguration` record: the required `auth` (an `AuthConfig` member, section 2.1), an optional `retryConfig` (section 2.3), and an optional `transportConfig` (section 2.4). The configuration is an included record parameter on both `init` methods, so callers pass its fields as named arguments, for example `new (auth = {accountName, accountKey})`.

### 2.3 Retry Configuration

The optional `RetryConfig` record shapes the retry behaviour of service requests; omitting it leaves the service defaults in place. Its fields:

* `retryPolicyType`: how the delay between tries grows, `EXPONENTIAL` or `FIXED_INTERVAL`. Defaults to `EXPONENTIAL`.
* `maxTries`: the maximum number of tries, counting the first attempt. Defaults to 4.
* `tryTimeoutSeconds`: the timeout applied to each individual try. Defaults to 60.
* `retryDelaySeconds`: the base delay between tries. Defaults to 4.
* `maxRetryDelaySeconds`: the upper bound on the delay between tries. Defaults to 120.
* `secondaryHostUrl`: a secondary endpoint to retry reads against, for geo redundant accounts. No default.

### 2.4 Transport Configuration

The optional `TransportConfig` record covers the HTTP transport: `proxy` routes the connector's traffic through an HTTP, SOCKS4, or SOCKS5 proxy, with optional credentials and a bypass list; `connectionPool` tunes the maximum number of concurrent connections and the idle, connect, and read timeouts; `secureSocket` configures custom TLS. The `SecureSocket` record carries trust material (a truststore or a PEM certificate path), a client identity for mutual TLS (a keystore or a certificate and key pair), the offered TLS versions and cipher suites, host name verification, session reuse, revocation checking, an SNI host name, and handshake and session timeouts.

## 3. AdminClient

The `AdminClient` manages the shares within a storage account. Use it for share lifecycle management, the account's file service configuration, and account level SAS tokens. For operations scoped to a single share, use the `Client`.

### 3.1 Initializing the AdminClient

The constructor takes the client configuration (section 2.2) as an included record parameter and validates the credential locally, with no call to Azure.

###### Example: Initializing the AdminClient

```ballerina
files:AdminClient admin = check new (auth = {accountName: "myacct", accountKey: "..."});
```

### 3.2 Share Management Operations

* `hasShare(shareName)`: returns whether the named share exists. The result is `false` only when Azure confirms absence (HTTP 404); an `Error` means the check itself could not complete, so an auth problem is never misreported as a missing share.
* `listShares(options)`: lists the shares of the account as a `ShareInfo` array. `ShareListOptions` offers a name prefix and toggles for including metadata, snapshots, and soft deleted shares.
* `createShare(shareName, options)`: creates a share. `ShareCreateOptions` accepts a quota, an access tier, the protocols to enable (SMB and/or NFS), the NFS root squash setting, and metadata.
* `deleteShare(shareName, options)`: deletes a share. When the account's soft delete retention policy is enabled, the share is retained for the configured period.
* `undeleteShare(shareName, version)`: restores a soft deleted share. Find restorable shares and their versions with `listShares({includeDeleted: true})`.

###### Example: Creating a Share When Absent

```ballerina
if !(check admin->hasShare("invoices")) {
    check admin->createShare("invoices", {quotaInGb: 100});
}
```

### 3.3 Service Configuration Operations

* `getServiceProperties()`: reads the account's file service configuration as a `ServiceProperties` record, covering request metrics collection, CORS rules, and protocol settings.
* `setServiceProperties(properties)`: writes the configuration. The service applies the record as a whole, so read the current configuration, modify it, and pass the result back.

### 3.4 User Delegation Key and Account SAS

* `getUserDelegationKey(startTime, expiryTime)`: obtains a `UserDelegationKey` for signing user delegation SAS tokens (section 4.9). Requires a client authenticated with Microsoft Entra ID whose identity holds the `Storage File Delegator` role. The key is valid at most 7 days.
* `generateAccountSas(values)`: mints an account level SAS token. This is an ordinary method, invoked with `.`: it signs the token locally with the account key and makes no service call. It requires a client authenticated with a shared key (or a connection string carrying an account key). Rotating the account key revokes every SAS minted from it.

## 4. Client

The `Client` is bound to a single share at initialization and operates on that share and the directories and files within it.

### 4.1 Initializing the Client

The constructor takes the share name and the client configuration (section 2.2). Binding is lazy: `init` makes no call to Azure, so initializing against a share that does not exist succeeds, and the first operation on it fails with a `NotFoundError`. The up front existence check is `AdminClient.hasShare`.

###### Example: Initializing the Client

```ballerina
files:Client fileShare = check new ("invoices", auth = {accountName: "myacct", accountKey: "..."});
```

### 4.2 Share Operations

* `getShareProperties()`: reads the bound share's properties, including its metadata, quota, access tier, and protocol settings, as a `ShareProperties` record.
* `setShareMetadata(metadata)`: replaces the share's complete metadata set. Metadata is free form, user defined annotation; Azure stores and returns it verbatim, and it is read back through `getShareProperties`.
* `getShareUsage()`: returns the share's approximate stored bytes.

### 4.3 Directory Operations

* `createDirectory(directoryPath, options)`: creates a directory. `DirectoryCreateOptions` accepts metadata.
* `deleteDirectory(directoryPath)`: deletes a directory, which must be empty.
* `hasDirectory(directoryPath)`: returns whether the directory exists, with the same semantics as `hasShare`: `false` only on a confirmed 404, an `Error` when the check itself fails.
* `getDirectoryProperties(directoryPath)`: reads the directory's properties as a `DirectoryProperties` record.
* `setDirectoryMetadata(directoryPath, metadata)`: replaces the directory's complete metadata set.
* `list(directoryPath, options)`: returns the directory's files and subdirectories as one lazy `Entry` stream, so memory stays bounded on large directories. Every `Entry` carries its full share relative `path` and an `isDirectory` flag. `ListOptions` offers a name prefix, recursion, page sizing (up to the service maximum of 5,000 entries per round trip), extended info (the entity tag and timestamps), and a `snapshotId` to list from a share snapshot.
* `renameDirectory(sourcePath, destinationPath, options)`: renames or moves a directory. The destination is a full share relative path, so `/X/A` to `/Y/A` re-parents within the same share. A directory can never overwrite an existing directory; with `RenameOptions.replaceIfExists` it may overwrite an existing file at the destination. Moving across shares is not possible.

###### Example: Listing a Directory Recursively

```ballerina
stream<files:Entry, files:Error?> entries = check fileShare->list("/2026", {recursive: true});
check entries.forEach(function(files:Entry entry) {
    io:println(entry.path);
});
```

### 4.4 File Operations

* `createFile(path, sizeInBytes, options)`: provisions an empty file of a fixed size; content is written separately through the transfer or range operations. `CreateOptions` accepts content headers and metadata.
* `deleteFile(path)`: deletes a file.
* `hasFile(path)`: returns whether the file exists, with the same semantics as `hasShare`.
* `getFileProperties(path)`: reads the file's properties, including its metadata, as a `FileProperties` record.
* `setFileMetadata(path, metadata)`: replaces the file's complete metadata set. Metadata is read via `getFileProperties`; only a setter is exposed.
* `setContentHeaders(path, headers)`: replaces the file's complete content header set (`Content-Type`, `Cache-Control`, and the other standard headers). Any header omitted from the record is cleared on the file.
* `renameFile(sourcePath, destinationPath, options)`: renames or moves a file. An existing destination file is overwritten only when `RenameOptions.replaceIfExists` is set; an existing destination directory always fails the operation.

### 4.5 Transfer Operations

* `uploadFromFile(sourcePath, destinationPath, options)`: copies a local file to the share. Neither this nor `download` deletes its source, and both take full paths including the file name, the local path first for the upload.
* `upload(content, destinationPath, options)`: uploads in-memory content (section 4.5.1).
* `uploadFromStream(content, contentLength, destinationPath, options)`: uploads a byte stream (section 4.5.2).
* `download(sourcePath, destinationPath, options)`: copies a share file to a local path, the share path first. The download fails with a client side `Error` when a local file already exists at the destination.
* `getFile(path, options, targetType)`: retrieves the file's content in the form the caller directed target type selects (section 4.5.3).

The upload options carry content headers and metadata; `upload` additionally accepts the `fileFormat` override described below. The retrieval options carry a byte `range`, a `snapshotId` to read from a share snapshot (section 4.8), and the `fileFormat` override for record shaped targets.

#### 4.5.1 In-Memory Content

`upload` takes its content as the `UploadContent` union. A `byte[]` is written as is, a `string` as raw text, and an `xml` value in its textual form.

A record (which includes any map of `anydata` members), a record array, or any other `json` value is serialized per a resolved format: the explicit `UploadContentOptions.fileFormat` override wins, else the destination path's extension (`.json`, `.xml`, `.csv`) decides. A record becomes a JSON or an XML document; a record array becomes CSV rows headed by the union of the records' field names in first seen order, with nil or absent members as empty cells and fields containing a comma, quote, backslash, or line break quoted in the dialect the CSV reads bind back; any other `json` value (an array, a scalar, or nil) becomes a JSON document. A record directed to CSV, a record array directed to a non CSV format, a non mapping `json` value directed to a non JSON format, and a format that resolves to neither an override nor a known extension are each refused with a client side `Error`.

There is no record stream upload, because the service pre-allocates a file at its full size before content is written; collect records into a `record {}[]` and upload them with `upload`, or use `uploadFromStream` for a byte stream of known length. CSV serialization takes record arrays only; to write positional or headerless rows, serialize them with your own code and upload the text.

###### Example: Uploading Records

```ballerina
type Metric record {
    string quarter;
    int revenue;
};

// The .json extension selects the JSON serialization.
Metric q1 = {quarter: "q1", revenue: 1250000};
check fileShare->upload(q1, "/2026/q1/metrics.json");

// A record array is CSV; the override beats the extension when they disagree.
Metric[] quarters = [{quarter: "q1", revenue: 1250000}, {quarter: "q2", revenue: 1310000}];
check fileShare->upload(quarters, "/2026/summary.dat", {fileFormat: files:CSV});
```

#### 4.5.2 Streaming Uploads

`uploadFromStream` requires `contentLength`, which must not be negative, because Azure Files pre-allocates the file at a fixed size before content is written into its ranges. Source chunks coalesce into range writes of the service's maximum range size (4 MiB), so the request count tracks the content size rather than the source's chunking, and memory stays bounded even when one source chunk exceeds the range size.

A source stream failure, a stream whose length does not match `contentLength`, and a negative length each surface as a client side `Error`, and every failure closes the source stream. A failed stream upload leaves the pre-allocated file, holding whatever ranges were written before the failure, at the destination; the connector does not delete it, so the caller can inspect it, overwrite it by uploading again, or delete it.

#### 4.5.3 Content Retrieval

`getFile` returns the file's content in the form the caller directed target type selects, so one operation covers every consumption shape:

* `byte[]`: the raw content, materialized in one call.
* `string`: the content decoded as UTF-8 text; content that is not valid UTF-8 fails with a client side `Error`.
* `json`: the content parsed as a JSON document.
* `xml`: the content parsed as an XML document.
* `record {}` or `record {}[]`: the content bound to the record shape per a resolved format. The explicit `GetFileOptions.fileFormat` override wins, else the path's extension (`.json`, `.xml`, `.csv`) decides. A single record binds from JSON or XML (never CSV), a record array binds from a JSON array or CSV rows (never XML), and a format that resolves to neither an override nor a known extension is refused with a client side `Error`, mirroring the `upload` refusals.
* `stream<byte[], error?>`: a lazy byte stream, so memory stays bounded for any file size.
* `stream<record {}, error?>`: CSV rows bound lazily, one record per pull; a row that fails to bind surfaces as the error entry of that pull.

The materialized targets download the full content before binding, and binding is strict: content that does not match the target type fails with a client side `Error`. CSV content binds to the record array and record stream targets only; to consume positional or headerless rows, retrieve the content as `string` or `byte[]` and parse it with the `data.csv` module. The listener's `laxDataBinding` setting applies only to listener handlers, not to these reads.

###### Example: Retrieving Content by Target Type

```ballerina
byte[] raw = check fileShare->getFile("/2026/q1/report.pdf");

Person[] people = check fileShare->getFile("/2026/q1/people.csv");

stream<byte[], error?> chunks = check fileShare->getFile("/2026/q1/large.bin");
```

###### Example: Working with Files

```ballerina
check fileShare->uploadFromFile("./invoice-2026-07.pdf", "/2026/07/invoice.pdf");

string text = check fileShare->getFile("/2026/07/notes.txt");

check fileShare->download("/2026/07/invoice.pdf", "./copies/invoice.pdf");
```

### 4.6 Copy Operations

* `copyFile(sourcePath, destinationPath, options)`: copies a file within the bound share under this client's credentials, returning a `CopyInfo`.
* `copyFileFromUrl(sourceUrl, destinationPath, options)`: copies from an external URL. A source in a different storage account, or any blob source, must carry its own authorization in the URL, typically a SAS token.
* `checkCopyStatus(path)`: reports the destination file's copy state as a `CopyStatusInfo`, or `()` when the file has never been a copy destination.
* `abortCopy(path, copyId)`: cancels a pending copy.

Copies are asynchronous: inspect the returned `CopyInfo.copyStatus` and, if pending, observe progress with `checkCopyStatus` or cancel with `abortCopy`. `CopyOptions` accepts destination metadata.

### 4.7 Range Operations

* `uploadRange(path, offset, content)`: writes a single byte range of at most 4 MiB and performs no chunking; for content of arbitrary size, use the transfer operations.
* `clearRange(path, offset, length)`: frees the underlying storage of a range. Storage deallocates in 512 byte units, so a smaller cleared span is zeroed but may still appear in `listRanges` until the whole unit is cleared.
* `listRanges(path, options)`: returns the valid (written) byte ranges of a file, each with inclusive start and end offsets.

### 4.8 Share Snapshot Operations

* `createShareSnapshot(metadata)`: creates a point in time, read only copy of the whole share and returns its `ShareSnapshotInfo`.
* `listShareSnapshots()`: lists the share's snapshots.
* `deleteShareSnapshot(snapshotId)`: deletes one snapshot.
* `listRangesDiff(path, previousSnapshotId, options)`: reports which of a file's ranges were written and which were cleared since a baseline snapshot, for incremental backup on top of snapshots.

Snapshot contents are read through the regular read operations: pass the snapshot id in the options of `download` or `getFile`, or the list options of `list`, to resolve the same paths inside the snapshot instead of the live share. The three snapshot management operations need account level credentials (an account key, a connection string carrying one, or an account SAS); a share scoped SAS is not sufficient.

### 4.9 SAS Generation

The SAS generation methods are ordinary methods, invoked with `.`: signing happens locally with the credential the client holds, and no call is made to Azure.

* `generateShareSas(values)`: mints a SAS token scoped to the whole share.
* `generateSas(path, values)`: mints a SAS token scoped to a single file.
* `generateShareUserDelegationSas(values, key)`: the share scoped user delegation variant.
* `generateUserDelegationSas(path, values, key)`: the file scoped user delegation variant.

`generateShareSas` and `generateSas` sign with the account key, so the client must be authenticated with a shared key (or a connection string carrying an account key); rotating the account key revokes every SAS minted from it. The signature values carry the validity window, the permissions, and optionally a protocol restriction, an IP range, or a stored access policy `identifier` in place of an explicit expiry and permissions. Generation fails with an `Error` when neither the identifier nor both `expiryTime` and `permissions` are supplied.

The user delegation variants sign with a `UserDelegationKey` (from `AdminClient.getUserDelegationKey`) instead of the account key, so no storage key is ever handled. They are valid at most 7 days (the key's lifetime), and stored access policies do not apply to them: the user delegation variants reject an `identifier` and require an explicit `expiryTime` and `permissions`.

###### Example: Minting a Read Only SAS for One File

```ballerina
import ballerina/time;

string sasToken = check fileShare.generateSas("/2026/07/invoice.pdf", {
    expiryTime: time:utcAddSeconds(time:utcNow(), 3600),
    permissions: {read: true}
});
```

## 5. The Listener and Caller

Azure Files is not exposed as an Event Grid source, so the listener polls. It uses stateless dispatch: each polling tick lists the watched path and reads each present file, invoking the content handler that matches it. No per file state is kept, so the contract is that handlers consume files by processing them and then deleting or moving them out of the watched path; an unprocessed file fires again on a later poll. This mode is trivially restart safe.

### 5.1 Initializing the Listener

The constructor takes the share name and the listener configuration as an included record parameter:

* `auth`: the authentication configuration (section 2.1). Required.
* `pollingInterval`: how often the watched path is polled, in seconds. Must be greater than zero; a non positive value fails `init`. Defaults to 60.
* `retryConfig` and `transportConfig`: forwarded to the listener's underlying client (sections 2.3 and 2.4).
* `laxDataBinding`: relaxed data binding for the typed content handlers (section 5.3). Defaults to false.

The listener builds one share scoped client at `init` and uses it for both its own polling and the `Caller` passed to handlers, so one listener holds exactly one connection stack. `init` also performs a one time XML parser setup for the XML content handlers; if that setup fails, `init` returns an error and the initialization can simply be retried.

The lifecycle methods follow the platform listener contract and return `error?`: `attach` accepts a single service and rejects a second one, `detach` of a service that is not attached fails, `'start` on a listener that is already running fails, and `gracefulStop` and `immediateStop` stop the polling schedule and return without waiting; handler invocations already running complete on their own threads in both cases.

###### Example: Reacting to Files Arriving on a Share

```ballerina
import ballerinax/azure.storage.files;

listener files:Listener invoiceListener = check new ("invoices",
    auth = {accountName: "myacct", accountKey: "..."},
    pollingInterval = 30
);

service /incoming on invoiceListener {
    remote function onFile(byte[] content, files:FileInfo file, files:Caller caller) returns error? {
        check caller->download(file.path, "./processed/" + file.name);
        // Consume the file so it does not fire again on the next poll.
        check caller->deleteFile(file.path);
    }
}
```

### 5.2 The Service and the Watched Path

One listener watches exactly one service and one path. The listener configuration carries the share level concerns (credentials, polling cadence, transport); what to watch is the service's attach point: `service /invoices on lsn` (a resource path, whose segments join with `/`) or `service "/dir one/reports" on lsn` (a string, for names a resource path cannot express). The path normalizes by trimming whitespace, collapsing repeated slashes, ensuring a leading slash, and stripping a trailing one. A service with no attach point watches the share root.

The optional `@files:ServiceConfig` annotation supplies per service filters; no annotation is needed for a service to work:

* `recursive`: whether the service watches subdirectories under the watched path. Defaults to true.
* `fileNamePattern`: a regular expression matched against the file name (not the path); non matching files are never dispatched. Attaching a service with an invalid pattern fails.
* `minFileAgeSeconds`: skip files younger than this many seconds, guarding against partial writes. No default.

To watch several paths, run several independent listeners. Overlap can still arise across separate listeners (a file under a path watched by two of them reaches each), so handling races there are the user's responsibility: idempotent handlers, or claiming a file by renaming it out of the watched path.

A credential that cannot list the watched path does not fail `attach`; the first poll surfaces the authorization error instead (section 5.5).

### 5.3 Content Handlers

A service declares at least one content handler, validated at compile time by the module's compiler plugin (at least one handler, each handler's parameter types and `error?` return, no resource functions, and no unknown remote methods).

* **`onFile`**: the raw bytes catch all. Takes its content as `byte[]` or as `stream<byte[], error?>`.
* **`onFileText`**: takes a `string`.
* **`onFileJson`**: takes a `json` value or a record. A `json` parameter receives any parsed root as is, and a record binds an object root by projection.
* **`onFileXml`**: takes an `xml` document or a record whose fields bind from the document's elements.
* **`onFileCsv`**: takes a string matrix (`string[][]`), a record array, a `stream<string[], error?>`, or a `stream<record {}, error?>`. The record forms map each row's fields through the file's first row, the header; the string forms keep every row of the file, the header row included.

The `FileInfo` and `Caller` parameters are optional trailing parameters: a handler declares its content parameter first, then either, both, or neither of `FileInfo` and `Caller` (with `FileInfo` before `Caller` when both are present), and the listener passes only what the handler declares. `FileInfo` carries what the directory listing provides: the share name, the share relative path, the file name, the size in bytes, the entity tag, and the last modified time.

Routing is by file extension: `txt` to `onFileText`, `json` to `onFileJson`, `xml` to `onFileXml`, `csv` to `onFileCsv`, and everything else to `onFile`. A per handler `@files:FunctionConfig` `fileNamePattern` overrides the extension routing. When more than one routing pattern matches a file name, the winner is fixed: patterns are checked in the order `onFileText`, `onFileJson`, `onFileXml`, `onFileCsv`, then `onFile`, so a typed handler's pattern always beats the catch all's. A file whose extension maps to an undeclared typed handler falls back to `onFile`, and is skipped and logged when `onFile` is absent too. A file routed to a typed handler whose content is malformed raises a `ContentBindingError` rather than falling through to `onFile` (section 5.5).

Binding is strict by default. Setting `laxDataBinding` on the listener relaxes it: JSON and CSV record binding treat a null value as an optional field and an absent member as a nilable field, and XML record binding tolerates elements the record does not declare.

The stream content forms read the file from the service in chunks as the handler drains the stream, instead of downloading it up front. A stream closes its underlying source at the end of the file, and a handler that abandons a stream early should call its `close()`. A CSV stream row that fails to bind surfaces as the error entry of that `next()` call, after which the stream is closed. The consume actions run on the handler's return exactly as for materialized content, so a handler that deletes or moves the file (or declares `afterProcess`) while its stream is not fully drained loses access to the remaining content.

### 5.4 Consuming Files

A handler can consume a file declaratively with the `@files:FunctionConfig` annotation's post processing actions, each either `DELETE` or a `Move` record:

* `afterProcess`: applied when the handler returns normally.
* `afterError`: applied when the handler returns an error or panics. It also covers the handler's content binding failures, but only when the service declares no `onError` handler; with `onError` declared, a binding failure is post processed by `onError`'s own annotation instead (section 5.5).

When neither is set, the file stays and fires again on a later poll. The annotation may also sit on `onError`, whose actions post process binding failures (section 5.5). A `Move` names the target directory in `moveTo` (the file keeps its name, and the directory is created if absent); on recursive watches, `preserveSubDirs` (default true) recreates the file's sub path under the target. A move onto an existing same named file replaces it, so a recurring file name moves cleanly every time; with `preserveSubDirs: false`, same named files from different subdirectories land on one destination name and the last move wins, so flattened moves should only be used where names are unique.

###### Example: Sorting Processed and Failed Drops

```ballerina
service /incoming on dropListener {
    @files:FunctionConfig {afterProcess: files:DELETE, afterError: {moveTo: "/failed"}}
    remote function onFileJson(Person person) returns error? {
        // A drop that binds is processed and deleted; one that does not bind moves to /failed.
    }
}
```

### 5.5 Error Notification

A service may declare an `onError` handler, `remote function onError(files:Error err, files:Caller caller?) returns error?`. It is notified on every listener side failure:

* A failed poll, with the mapped typed error, for example an `AuthorizationError` when the credential lacks access.
* A failed content read, with the mapped typed error: a file was listed but its content could not be downloaded for dispatch. The file stays for the next poll, so the notification repeats while the read keeps failing.
* A typed handler's content binding failure, with a `ContentBindingError` whose detail carries the file's share relative path in `filePath` and, when the content had been downloaded before binding failed, its raw bytes in `content` (section 6).

`onError` is not a content handler: it does not satisfy the at least one handler requirement and is never routed a file, so a `fileNamePattern` in its annotation is ignored. Errors returned by content handlers do not notify `onError`, and neither does a CSV stream row that fails to bind lazily (that error belongs to the handler draining the stream).

When `onError` is declared, a binding failure becomes its to handle: the file's fate follows `onError`'s own `@files:FunctionConfig`, with `afterProcess` applied when `onError` returns normally and `afterError` when it returns an error, and the content handler's `afterError` is not applied. With no annotation on `onError`, the file stays and fires again on a later poll. When `onError` is absent, a binding failure is logged and the content handler's `afterError` applies (section 5.4).

The consume actions cover binding failures only. A file whose read failed always stays for the next poll, whatever `onError` returns, since consuming a file that could not be read would discard content over a transient failure. A failed poll also logs its error, and polling keeps its configured interval, so the next scheduled poll scans again.

### 5.6 Delivery Semantics

Delivery is at least once. Polling runs on the platform task scheduler at the fixed `pollingInterval`, whose waiting policy makes a tick that fires during a still running scan wait for it; each matching file is dispatched to its handler on its own thread, so handlers run concurrently beyond the scan.

An in progress guard keyed on the file's path ensures one file is never dispatched to two invocations at once, so a file re-fires only after its previous handling has finished and it is still present. A file overwritten while its previous version is still being handled is dispatched with its new content on a later poll, once that handling completes. To keep that promise under auto consume, a configured `afterProcess` or `afterError` action first checks that the file's entity tag still matches the dispatched version and leaves a changed file for the next poll; the instant between that check and the action stays unguarded, since Azure Files offers no conditional deletes or renames.

A file overwritten in the short window between a poll's listing and its content read is delivered with the new content while the accompanying `FileInfo` still describes the listed version; Azure Files offers no conditional reads to close that window, and at least once delivery makes it harmless for handlers that treat `FileInfo` as advisory. Handlers should be idempotent, or claim a file by renaming it out of the watched path before processing.

### 5.7 The Caller

A `Caller` is passed to each handler so it can act on the event's file without constructing a separate client; it cannot be created by user code. It forwards a curated share scoped subset of the `Client`: `getFile`, `download`, `uploadFromFile`, `upload`, `deleteFile`, `copyFile`, `checkCopyStatus`, `abortCopy`, `renameFile`, `createDirectory`, `deleteDirectory`, and `list`, each with the semantics of its `Client` counterpart. Handlers pass the event's path explicitly, for example `caller->deleteFile(file.path)`, and read the share's name from `FileInfo.shareName`.

## 6. Errors

Every error raised by an operation of this module is a subtype of the distinct `Error` type. The hierarchy splits by origin: an error the Azure service raised is a `ServiceError` carrying the HTTP status and the Azure error code of the failed request in its detail, while a client side failure is the generic `Error` with no detail (no server exchange produced a status or a code, and the connector never fabricates them). The one client side exception is the listener's `ContentBindingError`, whose detail identifies the file that failed to bind. The service's human readable description becomes the Ballerina error's `message()`.

* **`Error`**: the root type, and the type of every client side failure.
  * **`ContentBindingError`**: a dispatched file's content did not bind to its handler's declared type, delivered to the service's `onError` handler (section 5.5). Its detail carries the file's share relative path in `filePath` and, when the content had been downloaded before binding failed, its raw bytes in `content`.
* **`ServiceError`**: any error raised by the Azure service, with `httpStatus` and `errorCode` in its detail. A service failure whose Azure error code maps to none of the subtypes below stays this generic type.
  * **`NotFoundError`**: the requested share, directory, or file was not found (HTTP 404).
  * **`ConflictError`**: the operation conflicts with the current state of the resource, for example creating a share that already exists (HTTP 409).
  * **`AuthorizationError`**: authentication or authorization failed, for example an invalid key or insufficient SAS permissions (HTTP 403).
  * **`PreconditionFailedError`**: a precondition was not met (HTTP 412), such as a lease held on the resource by another client blocking the operation.
  * **`RangeNotSatisfiableError`**: the requested byte range cannot be satisfied for the target file (HTTP 416).
  * **`QuotaExceededError`**: a write was rejected because the share's provisioned capacity is exhausted (HTTP 403).

The mapping keys on the Azure error code string, not the HTTP status alone: `ShareSizeLimitReached` (HTTP 403) maps to `QuotaExceededError`, distinct from auth failures (also HTTP 403) mapping to `AuthorizationError`. More specific error types should be checked before more general ones.

###### Example: Handling a Specific Failure

```ballerina
files:FileProperties|files:Error properties = fileShare->getFileProperties("/2026/07/invoice.pdf");
if properties is files:NotFoundError {
    // The file is absent; create it, or skip.
} else if properties is files:Error {
    return properties;
}
```

## 7. Observability

The module publishes metrics and traces that follow the unified file integration observability specification shared across all Ballerina file modules (FTP, SMB, Azure Files, and future modules). All metric names use the `file_` prefix, and the `module` tag distinguishes this module (`azure_files`) from others, so a single dashboard can monitor every file integration through shared panels filtered by module.

Observability is active only when the Ballerina runtime's observability subsystem is enabled (the `observabilityIncluded` flag in `Ballerina.toml` and the runtime configuration). When observability is disabled, no metrics or spans are created, and no overhead is introduced.

### 7.1 Metrics

#### Gauges

| Metric | Description |
|--------|-------------|
| `file_active_connections` | Number of open connections (client and listener). Incremented on `init`, decremented on listener stop. |
| `file_databinding_duration_seconds` | Time to fetch and convert file content into the handler's target type, in seconds. Configured with percentiles p50, p75, p90, p95, p99 over a 5-minute sliding window. |
| `file_resource_execution_duration_seconds` | Elapsed time of the handler method invocation, in seconds. Same percentile configuration as the data binding duration. |

The data binding duration covers the full pipeline: reading bytes from Azure Files and converting them to the handler's parameter type (e.g. `json`, `xml`, `record {}[]`). For streaming handlers, it measures stream creation time only; actual data transfer is lazy.

#### Explicit Counters

| Metric | Type | Description |
|--------|------|-------------|
| `file_bytes_transferred_total` | Counter | Bytes successfully read or written across operations. A sum of bytes, not a count of spans. Published for client materialized and streaming reads, client uploads, range writes, local-file transfers, and listener content reads. |
| `file_events_total` | Counter | Total file lifecycle and poll events. Every increment carries the same label keys, using `none` when not applicable: `action.type`, `file.stage`, `event.type`, `operation.type`, `outcome`, `error.type`, `failure.reason`, `handler.name`, `cleanup.action`, `watched.path`. |

#### Queried Metrics

These are derived from the `file_events_total` counter by filtering on tags:

| Logical metric | PromQL derivation |
|----------------|-------------------|
| Poll cycles | `file_events_total{action_type="poll_cycle"}` |
| Files found | `file_events_total{file_stage="found"}` |
| Files dispatched | `file_events_total{file_stage="dispatched"}` |
| Files skipped | `file_events_total{file_stage="found", outcome="skipped"}` |
| Files handled | `file_events_total{file_stage="handled"}` |
| Files cleaned up | `file_events_total{file_stage="cleaned_up"}` |
| Client operations | `requests_total_value{action_type="client_operation"}` |

### 7.2 Tags

All tags use the sentinel value `none` when not applicable for a given stage, rather than omitting the tag, to ensure consistent label sets across all increments of a metric.

#### Identity Tags

| Tag | Values | Scope | Notes |
|-----|--------|-------|-------|
| `module` | `azure_files` | All | Identifies this module. |
| `protocol` | `https`, `http` | All | Wire protocol, extracted from the service URL at init. |
| `type` | `client`, `listener` | All | Whether this is a client or listener operation. |
| `remote.url` | host or host:port | All | The Azure storage endpoint. |
| `watched.path` | Monitored directory path | Listener | Present on listener events and poll cycles only. |
| `host` | Local hostname | All | Hostname of the current instance; omitted if resolution fails. |

#### Action Tags

| Tag | Values | Scope | Notes |
|-----|--------|-------|-------|
| `action.type` | `poll_cycle`, `file_event`, `client_operation` | All | `poll_cycle` on poll completions, `file_event` on listener file lifecycle events, `client_operation` on client API calls. |
| `file.stage` | `found`, `dispatched`, `handled`, `cleaned_up` | Listener | Maps to the four-stage file lifecycle. |
| `event.type` | `create`, `delete`, `error` | Listener | Type of listener event. |
| `operation.type` | `get`, `put`, `manage` | All | `get` for read operations (getFile, download, list, getProperties, has*), `put` for write operations (upload, createFile, uploadRange), `manage` for administrative operations (delete, rename, copy, mkdir, setMetadata). |
| `handler.name` | Handler method name | Listener | Identifies which handler processed the file (e.g. `onFileJson`, `onFileCsv`). |
| `cleanup.action` | `move`, `delete` | Listener | Present on `file.stage=cleaned_up` events only. |

#### Outcome Tags

| Tag | Values | Scope | Notes |
|-----|--------|-------|-------|
| `outcome` | `success`, `failure`, `skipped` | All | Result of an operation. `skipped` indicates a file found but not matched to any handler. |
| `error.type` | Error type name | All | Only present when `outcome=failure`. Set to the Ballerina error type name (e.g. `NotFoundError`, `AuthorizationError`, `ContentBindingError`). |

#### File-Scoped Tags (Trace Only)

These tags appear on trace spans only, excluded from metrics to avoid cardinality explosion:

| Tag | Scope | Notes |
|-----|-------|-------|
| `file.path` | Traces | Full path of the file. On client operation spans and listener handler spans. |
| `destination.path` | Traces | Target path for rename, copy operations. |
| `file.size` | Traces | File size in bytes, on listener handler spans. |
| `file.modified_time` | Traces | Last-modified timestamp, on listener handler spans. |

### 7.3 File Lifecycle Tracing

Every file processed by the listener passes through up to four stages. A parent span (`file-lifecycle`) covers the entire lifecycle of a single file, and each stage produces metrics and, where a Ballerina method is invoked, a child span parented to it.

**Stage 1 — Found.** The listener's poll cycle discovers the file. If a handler matches, `file_events_total` is incremented with `file.stage=found`. If no handler matches, a single increment with `file.stage=found, outcome=skipped, error.type=no_handler_matched` is published and the file goes no further.

**Stage 2 — Dispatched.** The routing logic matches the file to a specific content handler. `file_events_total` is incremented with `file.stage=dispatched` and `handler.name` set to the matched handler method name.

**Stage 3 — Handled.** The file content is read from the service, converted to the expected Ballerina type, and the matched handler method is invoked. Two duration metrics are recorded: `file_databinding_duration_seconds` for the content fetch and conversion, and `file_resource_execution_duration_seconds` for the handler method execution. `file_events_total` is incremented with `file.stage=handled` and the outcome. The handler invocation creates a child span carrying trace-only file metadata (`file.path`, `file.size`, `file.modified_time`, `event.type=create`).

**Stage 4 — Cleaned Up.** After the handler completes, a post-processing action executes if configured via `@files:FunctionConfig`: either `delete` (remove the file) or `move` (move the file to a destination directory). `file_events_total` is incremented with `file.stage=cleaned_up`, `cleanup.action`, and the outcome. On failure, `error.type` is set to `delete_failed` or `move_failed`.

#### Client Operation Tracing

Every client operation enriches its auto-instrumented span with the identity tags (`module`, `type=client`, `remote.url`, `protocol`), the action tags (`action.type=client_operation`, `operation.type`), and trace-only file path tags. On completion, the span carries `outcome=success` or `outcome=failure` with the `error.type` tag naming the Ballerina error type.

#### Connection Tracking

The `file_active_connections` gauge is incremented when a `Client`, `AdminClient`, or `Listener` is initialized and decremented when a listener is stopped. The clients do not expose a `close` method because the Azure SDK manages connection pooling internally; the gauge tracks the number of live Ballerina client and listener instances rather than raw TCP connections.
