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

import com.azure.core.util.Context;
import com.azure.storage.file.share.models.ListSharesOptions;
import com.azure.storage.file.share.models.ShareFileRangeList;
import com.azure.storage.file.share.models.ShareItem;
import com.azure.storage.file.share.models.ShareSnapshotInfo;
import com.azure.storage.file.share.options.ShareFileListRangesDiffOptions;
import io.ballerina.lib.azure.storage.files.observability.AzureFilesMetricsUtil;
import io.ballerina.lib.azure.storage.files.observability.AzureFilesTracingUtil;
import io.ballerina.lib.azure.storage.files.util.BallerinaAzureClient;
import io.ballerina.lib.azure.storage.files.util.OptionsReader;
import io.ballerina.lib.azure.storage.files.util.RecordMapper;
import io.ballerina.lib.azure.storage.files.util.ValueUtils;
import io.ballerina.runtime.api.Environment;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;

/**
 * Native implementations of the {@code Client} share-snapshot operations. Creating a snapshot
 * is a share-level call; listing and deleting snapshots run through the service client because
 * the wire operations are service-scoped.
 */
public final class SnapshotOps {

    private SnapshotOps() {
    }

    /** Creates a snapshot of the bound share and returns its {@code ShareSnapshotInfo}. */
    public static Object createShareSnapshot(Environment env, BObject self, Object metadata) {
        AzureFilesTracingUtil.sendMetricsData(env, BallerinaAzureClient.getRemoteUrl(self),
                BallerinaAzureClient.getProtocol(self), AzureFilesMetricsUtil.OPERATION_TYPE_MANAGE, null);
        Object result = BallerinaAzureClient.invoke(env, () -> {
            ShareSnapshotInfo info = BallerinaAzureClient.getShareClient(self)
                    .createSnapshotWithResponse(ValueUtils.toStringMap(metadata), null, Context.NONE)
                    .getValue();
            return RecordMapper.shareSnapshotInfo(info.getSnapshot(), info.getETag(), info.getLastModified());
        });
        return AzureFilesTracingUtil.sendTraces(result, env);
    }

    /** Lists the bound share's snapshots as {@code ShareSnapshotInfo} records. */
    public static Object listShareSnapshots(Environment env, BObject self) {
        AzureFilesTracingUtil.sendMetricsData(env, BallerinaAzureClient.getRemoteUrl(self),
                BallerinaAzureClient.getProtocol(self), AzureFilesMetricsUtil.OPERATION_TYPE_GET, null);
        Object result = BallerinaAzureClient.invoke(env, () -> {
            String shareName = BallerinaAzureClient.getShareClient(self).getShareName();
            BArray snapshots = RecordMapper.recordArray(RecordMapper.RECORD_SHARE_SNAPSHOT_INFO);
            ListSharesOptions options = new ListSharesOptions()
                    .setPrefix(shareName)
                    .setIncludeSnapshots(true);
            for (ShareItem item : BallerinaAzureClient.getServiceClient(self).listShares(options, null, null)) {
                if (item.getName().equals(shareName) && item.getSnapshot() != null) {
                    snapshots.append(RecordMapper.shareSnapshotInfo(item.getSnapshot(),
                            item.getProperties().getETag(), item.getProperties().getLastModified()));
                }
            }
            return snapshots;
        });
        return AzureFilesTracingUtil.sendTraces(result, env);
    }

    /** Deletes one snapshot of the bound share. */
    public static Object deleteShareSnapshot(Environment env, BObject self, BString snapshotId) {
        AzureFilesTracingUtil.sendMetricsData(env, BallerinaAzureClient.getRemoteUrl(self),
                BallerinaAzureClient.getProtocol(self), AzureFilesMetricsUtil.OPERATION_TYPE_MANAGE, null);
        Object result = BallerinaAzureClient.invoke(env, () -> {
            String shareName = BallerinaAzureClient.getShareClient(self).getShareName();
            BallerinaAzureClient.getServiceClient(self).deleteShareWithResponse(shareName, snapshotId.getValue(), null,
                    Context.NONE);
            return null;
        });
        return AzureFilesTracingUtil.sendTraces(result, env);
    }

    /** Lists the ranges of a file that changed since a previous snapshot. */
    public static Object listRangesDiff(Environment env, BObject self, BString path,
            BString previousSnapshotId, Object options) {
        AzureFilesTracingUtil.sendMetricsData(env, BallerinaAzureClient.getRemoteUrl(self),
                BallerinaAzureClient.getProtocol(self), AzureFilesMetricsUtil.OPERATION_TYPE_GET, path.getValue());
        Object result = BallerinaAzureClient.invoke(env, () -> {
            ShareFileListRangesDiffOptions sdkOptions =
                    new ShareFileListRangesDiffOptions(previousSnapshotId.getValue());
            if (options != null) {
                @SuppressWarnings("unchecked")
                BMap<BString, Object> record = (BMap<BString, Object>) options;
                Object range = record.get(OptionsReader.RANGE);
                if (range != null) {
                    sdkOptions.setRange(OptionsReader.range(range));
                }
            }
            ShareFileRangeList list = FileOps.fileClient(self, path)
                    .listRangesDiffWithResponse(sdkOptions, null, Context.NONE)
                    .getValue();
            return RecordMapper.rangeDiff(list);
        });
        return AzureFilesTracingUtil.sendTraces(result, env);
    }
}
