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

import com.azure.storage.file.share.ShareClient;
import com.azure.storage.file.share.ShareDirectoryClient;
import com.azure.storage.file.share.models.ShareFileItem;
import com.azure.storage.file.share.options.ShareListFilesAndDirectoriesOptions;
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

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;

/**
 * Native backing of the lazy {@code Client.list} stream. Listing pages lazily through the SDK,
 * and recursive listing walks subdirectories depth-first as the consumer pulls entries.
 */
public final class ListOps {

    // Key under which the native iterator state is stored on a stream generator object.
    private static final String NATIVE_ITERATOR = "entryIterator";

    private ListOps() {
    }

    /**
     * Attaches a fresh listing iterator to the Ballerina stream generator object. Runs off the
     * scheduler via {@link BallerinaAzureClient#invoke} because the SDK's paged iterable fetches its first page
     * eagerly on construction.
     */
    public static Object newEntryIterator(Environment env, BObject self, BObject generator,
                                          BString directoryPath, BMap<BString, Object> options) {
        AzureFilesTracingUtil.sendMetricsData(env, BallerinaAzureClient.getRemoteUrl(self),
                BallerinaAzureClient.getProtocol(self), AzureFilesMetricsUtil.OPERATION_TYPE_GET,
                directoryPath.getValue());
        Object result = BallerinaAzureClient.invoke(env, () -> {
            String prefix = ValueUtils.optString(options, OptionsReader.PREFIX);
            boolean recursive = options.getBooleanValue(OptionsReader.RECURSIVE);
            Integer pageSize = Math.toIntExact((Long) options.get(OptionsReader.PAGE_SIZE));
            boolean extendedInfo = options.getBooleanValue(OptionsReader.INCLUDE_EXTENDED_INFO);
            String snapshotId = ValueUtils.optString(options, OptionsReader.SNAPSHOT_ID);
            EntryIterator iterator = new EntryIterator(BallerinaAzureClient.getShareClient(self, snapshotId),
                    BallerinaAzureClient.directoryPath(directoryPath), prefix, recursive, pageSize, extendedInfo);
            generator.addNativeData(NATIVE_ITERATOR, iterator);
            return null;
        });
        return AzureFilesTracingUtil.sendTraces(result, env);
    }

    /** Pulls the next entry: an {@code Entry} record, {@code null} at the end, or an error. */
    public static Object nextEntry(Environment env, BObject generator) {
        return BallerinaAzureClient.invoke(env, () -> {
            EntryIterator iterator = (EntryIterator) generator.getNativeData(NATIVE_ITERATOR);
            return iterator == null ? null : iterator.next();
        });
    }

    /** Stops an in-progress listing early. */
    public static Object closeEntryIterator(BObject generator) {
        generator.addNativeData(NATIVE_ITERATOR, null);
        return null;
    }

    /**
     * Depth-first walker over one directory subtree. Each stack frame is a directory whose SDK
     * listing is consumed lazily; entering a subdirectory pushes a frame, exhausting one pops it.
     */
    private static final class EntryIterator {

        private final ShareClient share;
        private final String prefix;
        private final boolean recursive;
        private final Integer pageSize;
        private final boolean extendedInfo;
        private final Deque<Frame> frames = new ArrayDeque<>();

        private EntryIterator(ShareClient share, String startPath, String prefix, boolean recursive,
                              Integer pageSize, boolean extendedInfo) {
            this.share = share;
            this.prefix = prefix;
            this.recursive = recursive;
            this.pageSize = pageSize;
            this.extendedInfo = extendedInfo;
            frames.push(new Frame(startPath, listing(startPath)));
        }

        private BMap<BString, Object> next() {
            while (!frames.isEmpty()) {
                Frame current = frames.peek();
                if (!current.items.hasNext()) {
                    frames.pop();
                    continue;
                }
                ShareFileItem item = current.items.next();
                BMap<BString, Object> entry = RecordMapper.entry(item, current.path);
                if (item.isDirectory() && recursive) {
                    String childPath = current.path.isEmpty() ? item.getName() : current.path + "/" + item.getName();
                    frames.push(new Frame(childPath, listing(childPath)));
                }
                return entry;
            }
            return null;
        }

        private Iterator<ShareFileItem> listing(String path) {
            ShareDirectoryClient directory = path.isEmpty()
                    ? share.getRootDirectoryClient() : share.getDirectoryClient(path);
            ShareListFilesAndDirectoriesOptions options = new ShareListFilesAndDirectoriesOptions()
                    .setPrefix(prefix)
                    .setMaxResultsPerPage(pageSize);
            if (extendedInfo) {
                options.setIncludeExtendedInfo(true)
                        .setIncludeTimestamps(true)
                        .setIncludeETag(true);
            }
            return directory.listFilesAndDirectories(options, null, null).iterator();
        }

        private record Frame(String path, Iterator<ShareFileItem> items) {
        }
    }
}
