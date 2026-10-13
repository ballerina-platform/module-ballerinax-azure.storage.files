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

import com.azure.storage.file.share.ShareDirectoryClient;
import com.azure.storage.file.share.options.ShareDirectoryCreateOptions;
import com.azure.storage.file.share.options.ShareFileRenameOptions;
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
 * Native implementations of the {@code Client} directory operations.
 */
public final class DirectoryOps {

    private DirectoryOps() {
    }

    /** Creates a directory with the given options. */
    public static Object createDirectory(Environment env, BObject self, BString directoryPath, Object options) {
        AzureFilesTracingUtil.sendMetricsData(env, BallerinaAzureClient.getRemoteUrl(self),
                BallerinaAzureClient.getProtocol(self), AzureFilesMetricsUtil.OPERATION_TYPE_MANAGE,
                directoryPath.getValue());
        Object result = BallerinaAzureClient.invoke(env, () -> {
            ShareDirectoryCreateOptions sdkOptions = new ShareDirectoryCreateOptions();
            if (options != null) {
                @SuppressWarnings("unchecked")
                BMap<BString, Object> record = (BMap<BString, Object>) options;
                sdkOptions.setMetadata(ValueUtils.optStringMap(record, OptionsReader.METADATA));
            }
            directoryClient(self, directoryPath).createWithResponse(sdkOptions, null, null);
            return null;
        });
        return AzureFilesTracingUtil.sendTraces(result, env);
    }

    /** Deletes an empty directory. */
    public static Object deleteDirectory(Environment env, BObject self, BString directoryPath) {
        AzureFilesTracingUtil.sendMetricsData(env, BallerinaAzureClient.getRemoteUrl(self),
                BallerinaAzureClient.getProtocol(self), AzureFilesMetricsUtil.OPERATION_TYPE_MANAGE,
                directoryPath.getValue());
        Object result = BallerinaAzureClient.invoke(env, () -> {
            directoryClient(self, directoryPath).delete();
            return null;
        });
        return AzureFilesTracingUtil.sendTraces(result, env);
    }

    /** Checks whether the directory exists; {@code false} only on a confirmed 404. */
    public static Object hasDirectory(Environment env, BObject self, BString directoryPath) {
        AzureFilesTracingUtil.sendMetricsData(env, BallerinaAzureClient.getRemoteUrl(self),
                BallerinaAzureClient.getProtocol(self), AzureFilesMetricsUtil.OPERATION_TYPE_GET,
                directoryPath.getValue());
        Object result = BallerinaAzureClient.invoke(env, () -> directoryClient(self, directoryPath).exists());
        return AzureFilesTracingUtil.sendTraces(result, env);
    }

    /** Fetches a directory's properties as a {@code DirectoryProperties} record. */
    public static Object getDirectoryProperties(Environment env, BObject self, BString directoryPath) {
        AzureFilesTracingUtil.sendMetricsData(env, BallerinaAzureClient.getRemoteUrl(self),
                BallerinaAzureClient.getProtocol(self), AzureFilesMetricsUtil.OPERATION_TYPE_GET,
                directoryPath.getValue());
        Object result = BallerinaAzureClient.invoke(env, () ->
                RecordMapper.directoryProperties(directoryClient(self, directoryPath).getProperties()));
        return AzureFilesTracingUtil.sendTraces(result, env);
    }

    /** Replaces a directory's user-defined metadata. */
    public static Object setDirectoryMetadata(Environment env, BObject self, BString directoryPath,
                                              BMap<BString, BString> metadata) {
        AzureFilesTracingUtil.sendMetricsData(env, BallerinaAzureClient.getRemoteUrl(self),
                BallerinaAzureClient.getProtocol(self), AzureFilesMetricsUtil.OPERATION_TYPE_MANAGE,
                directoryPath.getValue());
        Object result = BallerinaAzureClient.invoke(env, () -> {
            directoryClient(self, directoryPath).setMetadata(ValueUtils.toStringMap(metadata));
            return null;
        });
        return AzureFilesTracingUtil.sendTraces(result, env);
    }

    /** Renames or moves a directory within the share. */
    public static Object renameDirectory(Environment env, BObject self, BString sourcePath,
                                         BString destinationPath, Object options) {
        AzureFilesTracingUtil.sendMetricsData(env, BallerinaAzureClient.getRemoteUrl(self),
                BallerinaAzureClient.getProtocol(self), AzureFilesMetricsUtil.OPERATION_TYPE_MANAGE,
                sourcePath.getValue(), destinationPath.getValue());
        Object result = BallerinaAzureClient.invoke(env, () -> {
            String source = BallerinaAzureClient.filePath(sourcePath);
            String destination = BallerinaAzureClient.filePath(destinationPath);
            ShareDirectoryClient client = BallerinaAzureClient.getShareClient(self).getDirectoryClient(source);
            client.renameWithResponse(renameOptions(destination, options), null, null);
            return null;
        });
        return AzureFilesTracingUtil.sendTraces(result, env);
    }

    /** Builds the SDK rename options shared by the file and directory renames. */
    static ShareFileRenameOptions renameOptions(String destinationPath, Object options) {
        ShareFileRenameOptions sdkOptions = new ShareFileRenameOptions(destinationPath);
        if (options != null) {
            @SuppressWarnings("unchecked")
            BMap<BString, Object> record = (BMap<BString, Object>) options;
            sdkOptions.setReplaceIfExists(record.getBooleanValue(OptionsReader.REPLACE_IF_EXISTS))
                    .setMetadata(ValueUtils.optStringMap(record, OptionsReader.METADATA));
        }
        return sdkOptions;
    }

    /** Returns the SDK directory client for a path; the empty path addresses the share root. */
    static ShareDirectoryClient directoryClient(BObject self, BString directoryPath) {
        String path = BallerinaAzureClient.directoryPath(directoryPath);
        return path.isEmpty()
                ? BallerinaAzureClient.getShareClient(self).getRootDirectoryClient()
                : BallerinaAzureClient.getShareClient(self).getDirectoryClient(path);
    }
}
