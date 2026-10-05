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

import io.ballerina.lib.azure.storage.files.observability.AzureFilesMetricsUtil;
import io.ballerina.lib.azure.storage.files.observability.AzureFilesTracingUtil;
import io.ballerina.lib.azure.storage.files.util.BallerinaAzureClient;
import io.ballerina.lib.azure.storage.files.util.RecordMapper;
import io.ballerina.lib.azure.storage.files.util.ValueUtils;
import io.ballerina.runtime.api.Environment;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;

/**
 * Native implementations of the {@code Client} operations on the bound share itself.
 */
public final class ShareOps {

    private ShareOps() {
    }

    /** Fetches the bound share's properties as a {@code ShareProperties} record. */
    public static Object getShareProperties(Environment env, BObject self) {
        AzureFilesTracingUtil.sendMetricsData(env, BallerinaAzureClient.getRemoteUrl(self),
                BallerinaAzureClient.getProtocol(self), AzureFilesMetricsUtil.OPERATION_TYPE_GET, null);
        Object result = BallerinaAzureClient.invoke(env,
                () -> RecordMapper.shareProperties(BallerinaAzureClient.getShareClient(self).getProperties()));
        return AzureFilesTracingUtil.sendTraces(result, env);
    }

    /** Replaces the bound share's user-defined metadata. */
    public static Object setShareMetadata(Environment env, BObject self, BMap<BString, BString> metadata) {
        AzureFilesTracingUtil.sendMetricsData(env, BallerinaAzureClient.getRemoteUrl(self),
                BallerinaAzureClient.getProtocol(self), AzureFilesMetricsUtil.OPERATION_TYPE_MANAGE, null);
        Object result = BallerinaAzureClient.invoke(env, () -> {
            BallerinaAzureClient.getShareClient(self).setMetadata(ValueUtils.toStringMap(metadata));
            return null;
        });
        return AzureFilesTracingUtil.sendTraces(result, env);
    }

    /** Reports the bound share's current usage in bytes. */
    public static Object getShareUsage(Environment env, BObject self) {
        AzureFilesTracingUtil.sendMetricsData(env, BallerinaAzureClient.getRemoteUrl(self),
                BallerinaAzureClient.getProtocol(self), AzureFilesMetricsUtil.OPERATION_TYPE_GET, null);
        Object result = BallerinaAzureClient.invoke(env,
                () -> BallerinaAzureClient.getShareClient(self).getStatistics().getShareUsageInBytes());
        return AzureFilesTracingUtil.sendTraces(result, env);
    }
}
