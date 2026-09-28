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

import com.azure.core.util.polling.SyncPoller;
import com.azure.storage.file.share.ShareFileClient;
import com.azure.storage.file.share.models.ShareFileCopyInfo;
import com.azure.storage.file.share.models.ShareFileProperties;
import com.azure.storage.file.share.options.ShareFileCopyOptions;
import io.ballerina.lib.azure.storage.files.observability.AzureFilesMetricsUtil;
import io.ballerina.lib.azure.storage.files.observability.AzureFilesTracingUtil;
import io.ballerina.lib.azure.storage.files.util.BallerinaAzureClient;
import io.ballerina.lib.azure.storage.files.util.FilesErrorCreator;
import io.ballerina.lib.azure.storage.files.util.OptionsReader;
import io.ballerina.lib.azure.storage.files.util.RecordMapper;
import io.ballerina.lib.azure.storage.files.util.ValueUtils;
import io.ballerina.runtime.api.Environment;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;

import java.time.Duration;
import java.time.OffsetDateTime;

/**
 * Native implementations of the {@code Client} copy operations. Copies are asynchronous
 * server-side operations: starting one returns the initial status snapshot, and progress is
 * observed via {@code checkCopyStatus}.
 */
public final class CopyOps {

    private CopyOps() {
    }

    /** Starts a server-side copy from another file in the same share. */
    public static Object copyFile(Environment env, BObject self, BString sourcePath,
                                  BString destinationPath, Object options) {
        AzureFilesTracingUtil.sendMetricsData(env, BallerinaAzureClient.getRemoteUrl(self),
                BallerinaAzureClient.getProtocol(self), AzureFilesMetricsUtil.OPERATION_TYPE_MANAGE,
                sourcePath.getValue(), destinationPath.getValue());
        Object result = BallerinaAzureClient.invoke(env, () -> {
            String sourceUrl = BallerinaAzureClient.getShareClient(self)
                    .getFileClient(BallerinaAzureClient.filePath(sourcePath)).getFileUrl();
            return startCopy(self, sourceUrl, destinationPath, options);
        });
        return AzureFilesTracingUtil.sendTraces(result, env);
    }

    /** Starts a server-side copy from any accessible source URL. */
    public static Object copyFileFromUrl(Environment env, BObject self, BString sourceUrl,
                                         BString destinationPath, Object options) {
        AzureFilesTracingUtil.sendMetricsData(env, BallerinaAzureClient.getRemoteUrl(self),
                BallerinaAzureClient.getProtocol(self), AzureFilesMetricsUtil.OPERATION_TYPE_MANAGE,
                sourceUrl.getValue());
        Object result = BallerinaAzureClient.invoke(env,
                () -> startCopy(self, sourceUrl.getValue(), destinationPath, options));
        return AzureFilesTracingUtil.sendTraces(result, env);
    }

    /** Reports the progress of a copy targeting the given file; {@code null} when none exists. */
    public static Object checkCopyStatus(Environment env, BObject self, BString path) {
        AzureFilesTracingUtil.sendMetricsData(env, BallerinaAzureClient.getRemoteUrl(self),
                BallerinaAzureClient.getProtocol(self), AzureFilesMetricsUtil.OPERATION_TYPE_GET, path.getValue());
        Object result = BallerinaAzureClient.invoke(env, () ->
                RecordMapper.copyStatusInfo(FileOps.fileClient(self, path).getProperties()));
        return AzureFilesTracingUtil.sendTraces(result, env);
    }

    /** Aborts an in-progress copy identified by its copy id. */
    public static Object abortCopy(Environment env, BObject self, BString path, BString copyId) {
        AzureFilesTracingUtil.sendMetricsData(env, BallerinaAzureClient.getRemoteUrl(self),
                BallerinaAzureClient.getProtocol(self), AzureFilesMetricsUtil.OPERATION_TYPE_MANAGE, path.getValue());
        Object result = BallerinaAzureClient.invoke(env, () -> {
            FileOps.fileClient(self, path).abortCopy(copyId.getValue());
            return null;
        });
        return AzureFilesTracingUtil.sendTraces(result, env);
    }

    private static Object startCopy(BObject self, String sourceUrl, BString destinationPath, Object options) {
        ShareFileClient destination = FileOps.fileClient(self, destinationPath);
        ShareFileCopyOptions sdkOptions = new ShareFileCopyOptions();
        if (options != null) {
            @SuppressWarnings("unchecked")
            BMap<BString, Object> record = (BMap<BString, Object>) options;
            sdkOptions.setMetadata(ValueUtils.optStringMap(record, OptionsReader.METADATA));
        }
        SyncPoller<ShareFileCopyInfo, Void> poller =
                destination.beginCopy(sourceUrl, sdkOptions, Duration.ofSeconds(1));
        ShareFileCopyInfo info = poller.poll().getValue();
        if (info == null) {
            throw FilesErrorCreator.clientError("the copy operation returned no status", null);
        }
        // The poll cycle does not always carry the destination's eTag and last-modified time;
        // fill the gaps from the destination's properties so CopyInfo is always complete.
        String eTag = info.getETag();
        OffsetDateTime lastModified = info.getLastModified();
        if (eTag == null || lastModified == null) {
            ShareFileProperties properties = destination.getProperties();
            eTag = eTag == null ? properties.getETag() : eTag;
            lastModified = lastModified == null ? properties.getLastModified() : lastModified;
        }
        return RecordMapper.copyInfo(info.getCopyId(),
                info.getCopyStatus() == null ? RecordMapper.COPY_STATUS_PENDING : info.getCopyStatus().toString(),
                eTag, lastModified);
    }
}
