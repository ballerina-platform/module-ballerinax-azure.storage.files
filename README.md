# Ballerina Azure Files Connector

[![Build](https://github.com/ballerina-platform/module-ballerinax-azure.storage.files/actions/workflows/ci.yml/badge.svg)](https://github.com/ballerina-platform/module-ballerinax-azure.storage.files/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/ballerina-platform/module-ballerinax-azure.storage.files/branch/main/graph/badge.svg)](https://codecov.io/gh/ballerina-platform/module-ballerinax-azure.storage.files)
[![GitHub Last Commit](https://img.shields.io/github/last-commit/ballerina-platform/module-ballerinax-azure.storage.files.svg)](https://github.com/ballerina-platform/module-ballerinax-azure.storage.files/commits/main)
[![GraalVM Check](https://github.com/ballerina-platform/module-ballerinax-azure.storage.files/actions/workflows/build-with-bal-test-native.yml/badge.svg)](https://github.com/ballerina-platform/module-ballerinax-azure.storage.files/actions/workflows/build-with-bal-test-native.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

## Overview

Read, write, and manage files and shares in Azure Files, Microsoft's fully managed cloud file storage, and act on files as they arrive on a share.

[Azure Files](https://learn.microsoft.com/en-us/azure/storage/files/storage-files-introduction) offers fully managed file shares in the cloud, accessible via the industry-standard SMB and NFS protocols and a REST API.

The Azure Files connector offers APIs to connect to Azure Files and manage shares and the directories and files within them, covering uploads, downloads, copies, renames, byte ranges, snapshots, and SAS token generation. It also provides a polling `Listener` that turns files arriving on a share into service events.

### Key Features

- Share-scoped `Client` for directory and file operations, transfers, copies, and byte ranges
- Account-level `AdminClient` for creating, listing, deleting, and restoring shares
- Polling `Listener` that routes files arriving on a watched path to raw, typed, or streaming content handlers, with an optional `onError` error handler
- Share snapshots
- Authentication with shared key, SAS tokens, connection strings, and Microsoft Entra ID
- GraalVM compatible for native image builds

## Setup guide

To use the Azure Files connector, you must have an Azure subscription and an Azure storage account. If you do not have an Azure account, you can sign up for one [here](https://azure.microsoft.com/free/).

### Step 1: Create a storage account

1. Sign in to the [Azure portal](https://portal.azure.com/), search for **Storage accounts**, and open it.

2. Click **+ Create**.

    ![Create a storage account](https://raw.githubusercontent.com/ballerina-platform/module-ballerinax-azure.storage.files/main/docs/setup/resources/create-storage-account.png)

3. On the **Basics** tab, provide the following:

    | Input | Value |
    |-------|-------|
    | **Subscription** and **Resource group** | The subscription and group the account bills to. |
    | **Storage account name** | A globally unique name. |
    | **Region** | The region closest to your workload. |
    | **Performance** | **Standard** is sufficient for SMB file shares; choose **Premium** with the **File shares** account type only for provisioned performance or NFS. |

    ![Storage account basics](https://raw.githubusercontent.com/ballerina-platform/module-ballerinax-azure.storage.files/main/docs/setup/resources/storage-account-basics.png)

4. Click **Review + create**, then **Create**, and wait for the deployment to complete. For the full set of options, see the [Azure documentation](https://learn.microsoft.com/en-us/azure/storage/common/storage-account-create).

### Step 2: Create a file share

1. Open the deployed storage account and navigate to **Data storage** > **File shares**.

2. Click **+ File share**, provide a name, and click **Create**. The share name is what you pass to the connector's `Client` at initialization.

    ![Create a file share](https://raw.githubusercontent.com/ballerina-platform/module-ballerinax-azure.storage.files/main/docs/setup/resources/create-file-share.png)

### Step 3: Obtain the credentials

1. In the storage account, navigate to **Security + networking** > **Access keys**.

2. Click **Show** next to **key1** and copy the following values:

    | Value | Used as |
    |-------|---------|
    | Storage account name | `accountName` |
    | key1 **Key** | `accountKey` |

    ![Copy the access key](https://raw.githubusercontent.com/ballerina-platform/module-ballerinax-azure.storage.files/main/docs/setup/resources/access-keys.png)

3. Optionally, use one of the other credentials the connector accepts: a SAS token or SAS URL (generated under **Security + networking** > **Shared access signature**), a connection string (shown alongside each access key), or Microsoft Entra ID credentials.

    ![Generate a SAS token](https://raw.githubusercontent.com/ballerina-platform/module-ballerinax-azure.storage.files/main/docs/setup/resources/generate-sas.png)

## Quickstart

To use the `azure.storage.files` connector in your Ballerina application, modify the `.bal` file as follows:

### Step 1: Import the module

```ballerina
import ballerinax/azure.storage.files;
```

### Step 2: Instantiate a new connector

A `Client` is bound to a single file share. Provide the credentials through configurable variables:

```ballerina
configurable string accountName = ?;
configurable string accountKey = ?;

files:Client fileClient = check new ("reports", auth = {accountName, accountKey});
```

### Step 3: Invoke the connector operation

Now, utilize the available connector operations.

#### Create the share

The client is bound to a share, so create it first if it does not exist yet.

```ballerina
files:AdminClient admin = check new (auth = {accountName, accountKey});
check admin->createShare("reports");
```

#### Upload a file

Paths are relative to the bound share, so `/q1.pdf` is at the share root. Azure does not create
parent directories, so create a directory before writing into one.

```ballerina
check fileClient->uploadFromFile("./local/q1.pdf", "/q1.pdf");
```

#### Get the properties of a file

```ballerina
files:FileProperties props = check fileClient->getFileProperties("/q1.pdf");
```

### Step 4: Run the Ballerina application

Save the changes and run the Ballerina application using the following command.

```bash
bal run
```

## Examples

The `azure.storage.files` connector provides practical examples illustrating usage in various scenarios. Explore these [examples](https://github.com/ballerina-platform/module-ballerinax-azure.storage.files/tree/main/examples), covering use cases like backing up a folder to a share, handing out a time-limited file link, and processing files dropped into a share folder.

1. [File backup](https://github.com/ballerina-platform/module-ballerinax-azure.storage.files/tree/main/examples/file-backup) - Back up a local folder to a file share and restore a file from it.
2. [Share handout](https://github.com/ballerina-platform/module-ballerinax-azure.storage.files/tree/main/examples/share-handout) - Upload a report and generate a time-limited, read-only SAS URL to share with a third party.
3. [Drop folder processor](https://github.com/ballerina-platform/module-ballerinax-azure.storage.files/tree/main/examples/drop-folder-processor) - Watch a folder on a share with the listener and process each dropped file, deleting JSON files and moving the rest into a processed folder.
4. [Change tracker](https://github.com/ballerina-platform/module-ballerinax-azure.storage.files/tree/main/examples/change-tracker) - Derive created, modified, and deleted events from a watched share with an application-kept eTag snapshot.

## Issues and projects

The **Issues** and **Projects** tabs are disabled for this repository as this is part of the Ballerina library. To report bugs, request new features, start new discussions, view project boards, etc., visit the Ballerina library [parent repository](https://github.com/ballerina-platform/ballerina-library).

This repository only contains the source code for the package.

## Build from the source

### Setting up the prerequisites

1. Download and install Java SE Development Kit (JDK) version 25. You can download it from either of the following sources:

    * [Oracle JDK](https://www.oracle.com/java/technologies/downloads/)
    * [OpenJDK](https://adoptium.net/)

   > **Note:** After installation, remember to set the `JAVA_HOME` environment variable to the directory where JDK was installed.

2. Download and install [Ballerina Swan Lake](https://ballerina.io/).

3. Export Github Personal access token with read package permissions as follows,

    ```bash
    export packageUser=<Username>
    export packagePAT=<Personal access token>
    ```

### Build options

Execute the commands below to build from the source.

1. To build the package:

    ```bash
    ./gradlew clean build
    ```

2. To run the tests:

   ```bash
   ./gradlew clean test
   ```

3. To build the without the tests:

   ```bash
   ./gradlew clean build -x test
   ```

4. To run the tests against different environments:

   The suite runs against an in-process mock service by default and against a live Azure storage account when credentials are provided. See the [Test Guide](ballerina/tests/README.md) for details.

5. To debug package with a remote debugger:

   ```bash
   ./gradlew clean build -Pdebug=<port>
   ```

6. To debug with the Ballerina language:

   ```bash
   ./gradlew clean build -PbalJavaDebug=<port>
   ```

7. Publish the generated artifacts to the local Ballerina Central repository:

    ```bash
    ./gradlew clean build -PpublishToLocalCentral=true
    ```

8. Publish the generated artifacts to the Ballerina Central repository:

   ```bash
   ./gradlew clean build -PpublishToCentral=true
   ```

## Contribute to Ballerina

As an open-source project, Ballerina welcomes contributions from the community.

For more information, go to the [contribution guidelines](https://github.com/ballerina-platform/ballerina-lang/blob/master/CONTRIBUTING.md).

## Code of conduct

All the contributors are encouraged to read the [Ballerina Code of Conduct](https://ballerina.io/code-of-conduct).

## Useful links

* For more information go to the [`azure.storage.files` package](https://lib.ballerina.io/ballerinax/azure.storage.files/latest).
* For example demonstrations of the usage, go to [Ballerina By Examples](https://ballerina.io/learn/by-example/).
* Chat live with us via our [Discord server](https://discord.gg/ballerinalang).
* Post all technical questions on Stack Overflow with the [#ballerina](https://stackoverflow.com/questions/tagged/ballerina) tag.
