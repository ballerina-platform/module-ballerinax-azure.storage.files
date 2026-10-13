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

import com.azure.storage.file.share.models.ShareFileRange;
import com.azure.storage.file.share.models.ShareFileUploadRangeOptions;
import io.ballerina.lib.azure.storage.files.observability.AzureFilesMetricsUtil;
import io.ballerina.lib.azure.storage.files.observability.AzureFilesTracingUtil;
import io.ballerina.lib.azure.storage.files.util.BallerinaAzureClient;
import io.ballerina.lib.azure.storage.files.util.OptionsReader;
import io.ballerina.lib.azure.storage.files.util.RecordMapper;
import io.ballerina.runtime.api.Environment;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;

import java.io.ByteArrayInputStream;

/**
 * Native implementations of the low-level {@code Client} range operations.
 */
public final class RangeOps {

    private RangeOps() {
    }

    /** Writes bytes into an existing file at the given offset. */
    public static Object uploadRange(Environment env, BObject self, BString path, long offset, BArray content) {
        String url = BallerinaAzureClient.getRemoteUrl(self);
        String protocol = BallerinaAzureClient.getProtocol(self);
        AzureFilesTracingUtil.sendMetricsData(env, url, protocol, AzureFilesMetricsUtil.OPERATION_TYPE_PUT,
                path.getValue());
        Object result = BallerinaAzureClient.invoke(env, () -> {
            byte[] bytes = content.getBytes();
            FileOps.fileClient(self, path).uploadRangeWithResponse(
                    new ShareFileUploadRangeOptions(new ByteArrayInputStream(bytes), bytes.length)
                            .setOffset(offset),
                    null, null);
            return null;
        });
        if (!(result instanceof BError)) {
            AzureFilesMetricsUtil.reportBytesTransferred(url, protocol, AzureFilesMetricsUtil.CONTEXT_CLIENT,
                    AzureFilesMetricsUtil.OPERATION_TYPE_PUT, content.getLength());
        }
        return AzureFilesTracingUtil.sendTraces(result, env);
    }

    /** Clears (zeroes) a byte range of an existing file. */
    public static Object clearRange(Environment env, BObject self, BString path, long offset, long length) {
        AzureFilesTracingUtil.sendMetricsData(env, BallerinaAzureClient.getRemoteUrl(self),
                BallerinaAzureClient.getProtocol(self), AzureFilesMetricsUtil.OPERATION_TYPE_MANAGE, path.getValue());
        Object result = BallerinaAzureClient.invoke(env, () -> {
            FileOps.fileClient(self, path).clearRangeWithResponse(length, offset, null, null);
            return null;
        });
        return AzureFilesTracingUtil.sendTraces(result, env);
    }

    /** Lists the valid (written) byte ranges of a file as {@code Range} records. */
    public static Object listRanges(Environment env, BObject self, BString path, Object options) {
        AzureFilesTracingUtil.sendMetricsData(env, BallerinaAzureClient.getRemoteUrl(self),
                BallerinaAzureClient.getProtocol(self), AzureFilesMetricsUtil.OPERATION_TYPE_GET, path.getValue());
        Object result = BallerinaAzureClient.invoke(env, () -> {
            ShareFileRange range = null;
            if (options != null) {
                @SuppressWarnings("unchecked")
                BMap<BString, Object> record = (BMap<BString, Object>) options;
                range = OptionsReader.range(record.get(OptionsReader.RANGE));
            }
            BArray rangeArray = RecordMapper.recordArray(RecordMapper.RECORD_RANGE);
            for (ShareFileRange r : FileOps.fileClient(self, path).listRanges(range, null, null)) {
                rangeArray.append(RecordMapper.range(r));
            }
            return rangeArray;
        });
        return AzureFilesTracingUtil.sendTraces(result, env);
    }
}
