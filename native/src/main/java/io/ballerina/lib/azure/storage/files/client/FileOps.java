/*
 * Copyright (c) 2026, WSO2 LLC. (http://www.wso2.com).
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.ballerina.lib.azure.storage.files.client;

import com.azure.storage.file.share.ShareFileClient;
import com.azure.storage.file.share.models.ShareFileHttpHeaders;
import com.azure.storage.file.share.options.ShareFileCreateOptions;
import io.ballerina.lib.azure.storage.files.observability.AzureFilesMetricsUtil;
import io.ballerina.lib.azure.storage.files.observability.AzureFilesTracingUtil;
import io.ballerina.lib.azure.storage.files.util.BallerinaAzureClient;
import io.ballerina.lib.azure.storage.files.util.OptionsReader;
import io.ballerina.lib.azure.storage.files.util.RecordMapper;
import io.ballerina.lib.azure.storage.files.util.ValueUtils;
import io.ballerina.runtime.api.Environment;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;

/**
 * Native implementations of the basic {@code Client} file operations.
 */
public final class FileOps {

    private FileOps() {
    }

    /** Creates an empty file pre-allocated to the given size. */
    public static Object createFile(Environment env, BObject self, BString path, long sizeInBytes, Object options) {
        AzureFilesTracingUtil.sendMetricsData(env, BallerinaAzureClient.getRemoteUrl(self),
                BallerinaAzureClient.getProtocol(self), AzureFilesMetricsUtil.OPERATION_TYPE_PUT, path.getValue());
        Object result = BallerinaAzureClient.invoke(env, () -> {
            fileClient(self, path).createWithResponse(createOptions(sizeInBytes, options), null, null);
            return null;
        });
        return AzureFilesTracingUtil.sendTraces(result, env);
    }

    /** Deletes a file. */
    public static Object deleteFile(Environment env, BObject self, BString path) {
        AzureFilesTracingUtil.sendMetricsData(env, BallerinaAzureClient.getRemoteUrl(self),
                BallerinaAzureClient.getProtocol(self), AzureFilesMetricsUtil.OPERATION_TYPE_MANAGE, path.getValue());
        Object result = BallerinaAzureClient.invoke(env, () -> {
            fileClient(self, path).delete();
            return null;
        });
        return AzureFilesTracingUtil.sendTraces(result, env);
    }

    /** Checks whether the file exists; {@code false} only on a confirmed 404. */
    public static Object hasFile(Environment env, BObject self, BString path) {
        AzureFilesTracingUtil.sendMetricsData(env, BallerinaAzureClient.getRemoteUrl(self),
                BallerinaAzureClient.getProtocol(self), AzureFilesMetricsUtil.OPERATION_TYPE_GET, path.getValue());
        Object result = BallerinaAzureClient.invoke(env, () -> fileClient(self, path).exists());
        return AzureFilesTracingUtil.sendTraces(result, env);
    }

    /** Fetches a file's properties as a {@code FileProperties} record. */
    public static Object getFileProperties(Environment env, BObject self, BString path) {
        AzureFilesTracingUtil.sendMetricsData(env, BallerinaAzureClient.getRemoteUrl(self),
                BallerinaAzureClient.getProtocol(self), AzureFilesMetricsUtil.OPERATION_TYPE_GET, path.getValue());
        Object result = BallerinaAzureClient.invoke(env,
                () -> RecordMapper.fileProperties(fileClient(self, path).getProperties()));
        return AzureFilesTracingUtil.sendTraces(result, env);
    }

    /** Replaces a file's user-defined metadata. */
    public static Object setFileMetadata(Environment env, BObject self, BString path, BMap<BString, BString> metadata) {
        AzureFilesTracingUtil.sendMetricsData(env, BallerinaAzureClient.getRemoteUrl(self),
                BallerinaAzureClient.getProtocol(self), AzureFilesMetricsUtil.OPERATION_TYPE_MANAGE, path.getValue());
        Object result = BallerinaAzureClient.invoke(env, () -> {
            fileClient(self, path).setMetadata(ValueUtils.toStringMap(metadata));
            return null;
        });
        return AzureFilesTracingUtil.sendTraces(result, env);
    }

    /** Replaces a file's HTTP content headers, keeping its current size. */
    public static Object setContentHeaders(Environment env, BObject self, BString path, BMap<BString, Object> headers) {
        AzureFilesTracingUtil.sendMetricsData(env, BallerinaAzureClient.getRemoteUrl(self),
                BallerinaAzureClient.getProtocol(self), AzureFilesMetricsUtil.OPERATION_TYPE_MANAGE, path.getValue());
        Object result = BallerinaAzureClient.invoke(env, () -> {
            ShareFileClient client = fileClient(self, path);
            long currentSize = client.getProperties().getContentLength();
            ShareFileHttpHeaders sdkHeaders = OptionsReader.contentHeaders(headers);
            client.setProperties(currentSize, sdkHeaders, null, null);
            return null;
        });
        return AzureFilesTracingUtil.sendTraces(result, env);
    }

    /** Renames or moves a file within the share. */
    public static Object renameFile(Environment env, BObject self, BString sourcePath,
                                    BString destinationPath, Object options) {
        AzureFilesTracingUtil.sendMetricsData(env, BallerinaAzureClient.getRemoteUrl(self),
                BallerinaAzureClient.getProtocol(self), AzureFilesMetricsUtil.OPERATION_TYPE_MANAGE,
                sourcePath.getValue(), destinationPath.getValue());
        Object result = BallerinaAzureClient.invoke(env, () -> {
            String source = BallerinaAzureClient.filePath(sourcePath);
            String destination = BallerinaAzureClient.filePath(destinationPath);
            BallerinaAzureClient.getShareClient(self).getFileClient(source)
                    .renameWithResponse(DirectoryOps.renameOptions(destination, options), null, null);
            return null;
        });
        return AzureFilesTracingUtil.sendTraces(result, env);
    }

    /** Builds the SDK create options shared by createFile and the upload operations. */
    static ShareFileCreateOptions createOptions(long sizeInBytes, Object options) {
        ShareFileCreateOptions sdkOptions = new ShareFileCreateOptions(sizeInBytes);
        if (options != null) {
            @SuppressWarnings("unchecked")
            BMap<BString, Object> record = (BMap<BString, Object>) options;
            sdkOptions.setShareFileHttpHeaders(OptionsReader.contentHeaders(record.get(OptionsReader.CONTENT_HEADERS)))
                    .setMetadata(ValueUtils.optStringMap(record, OptionsReader.METADATA));
        }
        return sdkOptions;
    }

    /** Returns the SDK file client for a combined share-relative path. */
    static ShareFileClient fileClient(BObject self, BString path) {
        return BallerinaAzureClient.getShareClient(self).getFileClient(BallerinaAzureClient.filePath(path));
    }

    /** Returns the SDK file client for a path, bound to a share snapshot when an id is given. */
    static ShareFileClient fileClient(BObject self, BString path, String snapshotId) {
        return BallerinaAzureClient.getShareClient(self, snapshotId).getFileClient(BallerinaAzureClient.filePath(path));
    }
}
