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
import com.azure.storage.file.share.StorageFileInputStream;
import com.azure.storage.file.share.models.ShareFileRange;
import com.azure.storage.file.share.models.ShareFileUploadRangeOptions;
import io.ballerina.lib.azure.storage.files.observability.AzureFilesMetricsUtil;
import io.ballerina.lib.azure.storage.files.observability.AzureFilesTracingUtil;
import io.ballerina.lib.azure.storage.files.util.BallerinaAzureClient;
import io.ballerina.lib.azure.storage.files.util.FilesErrorCreator;
import io.ballerina.lib.azure.storage.files.util.OptionsReader;
import io.ballerina.runtime.api.Environment;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.types.ArrayType;
import io.ballerina.runtime.api.types.TypeTags;
import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.api.values.BXml;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Arrays;

/**
 * Native implementations of the {@code Client} transfer operations: local-file upload and
 * download, in-memory content upload, and the two content streams (upload from a Ballerina
 * stream, download as a Ballerina stream).
 */
public final class TransferOps {

    // Key under which an open content input stream is stored on a stream generator object.
    private static final String NATIVE_INPUT_STREAM = "inputStream";
    static final String NATIVE_REMOTE_URL = "observabilityRemoteUrl";
    static final String NATIVE_PROTOCOL = "observabilityProtocol";

    private TransferOps() {
    }

    /**
     * The service's maximum size for one range write: 4 MiB. Put Range rejects larger ranges
     * with HTTP 413 (learn.microsoft.com/rest/api/storageservices/put-range); the SDK exposes
     * no public constant for the limit.
     */
    private static final int MAX_RANGE_BYTES = 4 * 1024 * 1024;
    /** The chunk size handed to Ballerina byte-stream consumers. */
    private static final int READ_CHUNK_BYTES = 64 * 1024;

    /** Uploads a local file to the share, creating the destination at the source's size. */
    public static Object uploadFromFile(Environment env, BObject self, BString sourcePath,
                                    BString destinationPath, Object options) {
        AzureFilesTracingUtil.sendMetricsData(env, BallerinaAzureClient.getRemoteUrl(self),
                BallerinaAzureClient.getProtocol(self), AzureFilesMetricsUtil.OPERATION_TYPE_PUT,
                destinationPath.getValue());
        Object result = BallerinaAzureClient.invoke(env, () -> {
            Path localPath = Path.of(sourcePath.getValue());
            long size;
            try {
                size = Files.size(localPath);
            } catch (NoSuchFileException e) {
                throw FilesErrorCreator.clientError(
                        "local file not found: " + sourcePath.getValue(), e);
            } catch (IOException e) {
                throw FilesErrorCreator.clientError(BallerinaAzureClient.describe(e), e);
            }
            ShareFileClient client = FileOps.fileClient(self, destinationPath);
            client.createWithResponse(FileOps.createOptions(size, options), null, null);
            client.uploadFromFile(localPath.toString());
            AzureFilesMetricsUtil.reportBytesTransferred(BallerinaAzureClient.getRemoteUrl(self),
                    BallerinaAzureClient.getProtocol(self), AzureFilesMetricsUtil.CONTEXT_CLIENT,
                    AzureFilesMetricsUtil.OPERATION_TYPE_PUT, size);
            return null;
        });
        return AzureFilesTracingUtil.sendTraces(result, env);
    }

    /**
     * Uploads in-memory content (bytes, text, an XML document, or CSV rows) as a new file.
     * Record content never reaches this call: it is serialized on the Ballerina side first.
     */
    public static Object upload(Environment env, BObject self, Object content,
                                       BString destinationPath, Object options) {
        AzureFilesTracingUtil.sendMetricsData(env, BallerinaAzureClient.getRemoteUrl(self),
                BallerinaAzureClient.getProtocol(self), AzureFilesMetricsUtil.OPERATION_TYPE_PUT,
                destinationPath.getValue());
        Object result = BallerinaAzureClient.invoke(env, () -> {
            byte[] bytes = contentBytes(content);
            ShareFileClient client = FileOps.fileClient(self, destinationPath);
            client.createWithResponse(FileOps.createOptions(bytes.length, options), null, null);
            if (bytes.length > 0) {
                client.upload(new ByteArrayInputStream(bytes), bytes.length, null);
            }
            AzureFilesMetricsUtil.reportBytesTransferred(BallerinaAzureClient.getRemoteUrl(self),
                    BallerinaAzureClient.getProtocol(self), AzureFilesMetricsUtil.CONTEXT_CLIENT,
                    AzureFilesMetricsUtil.OPERATION_TYPE_PUT, bytes.length);
            return null;
        });
        return AzureFilesTracingUtil.sendTraces(result, env);
    }

    /** Creates the pre-allocated destination file for a stream upload. */
    public static Object prepareStreamUpload(Environment env, BObject self, BString destinationPath,
                                             long contentLength, Object options) {
        AzureFilesTracingUtil.sendMetricsData(env, BallerinaAzureClient.getRemoteUrl(self),
                BallerinaAzureClient.getProtocol(self), AzureFilesMetricsUtil.OPERATION_TYPE_PUT,
                destinationPath.getValue());
        Object result = BallerinaAzureClient.invoke(env, () -> {
            FileOps.fileClient(self, destinationPath)
                    .createWithResponse(FileOps.createOptions(contentLength, options), null, null);
            return null;
        });
        return AzureFilesTracingUtil.sendTraces(result, env);
    }

    /** Writes one stream chunk at the given offset, splitting it into service-compliant ranges. */
    public static Object writeStreamChunk(Environment env, BObject self, BString destinationPath,
                                          long offset, BArray chunk) {
        AzureFilesTracingUtil.sendMetricsData(env, BallerinaAzureClient.getRemoteUrl(self),
                BallerinaAzureClient.getProtocol(self), AzureFilesMetricsUtil.OPERATION_TYPE_PUT,
                destinationPath.getValue());
        Object result = BallerinaAzureClient.invoke(env, () -> {
            byte[] bytes = chunk.getBytes();
            ShareFileClient client = FileOps.fileClient(self, destinationPath);
            long position = offset;
            int written = 0;
            while (written < bytes.length) {
                int length = Math.min(bytes.length - written, MAX_RANGE_BYTES);
                client.uploadRangeWithResponse(new ShareFileUploadRangeOptions(
                                new ByteArrayInputStream(bytes, written, length), length)
                                .setOffset(position),
                        null, null);
                written += length;
                position += length;
            }
            AzureFilesMetricsUtil.reportBytesTransferred(BallerinaAzureClient.getRemoteUrl(self),
                    BallerinaAzureClient.getProtocol(self), AzureFilesMetricsUtil.CONTEXT_CLIENT,
                    AzureFilesMetricsUtil.OPERATION_TYPE_PUT, bytes.length);
            return null;
        });
        return AzureFilesTracingUtil.sendTraces(result, env);
    }

    /** Downloads a share file (or a range of it) to a local file. */
    public static Object download(Environment env, BObject self, BString sourcePath,
                                      BString destinationPath, Object options) {
        AzureFilesTracingUtil.sendMetricsData(env, BallerinaAzureClient.getRemoteUrl(self),
                BallerinaAzureClient.getProtocol(self), AzureFilesMetricsUtil.OPERATION_TYPE_GET,
                sourcePath.getValue());
        Object result = BallerinaAzureClient.invoke(env, () -> {
            OptionsReader.DownloadArgs args = OptionsReader.downloadArgs(options);
            ShareFileClient client = FileOps.fileClient(self, sourcePath, args.snapshotId());
            try {
                if (args.range() == null) {
                    client.downloadToFile(destinationPath.getValue());
                } else {
                    client.downloadToFileWithResponse(destinationPath.getValue(),
                            downloadToFileRange(OptionsReader.range(args.range())), null, null);
                }
                long transferredBytes;
                try {
                    transferredBytes = Files.size(Path.of(destinationPath.getValue()));
                } catch (IOException e) {
                    throw FilesErrorCreator.clientError(
                            "cannot inspect local file " + destinationPath.getValue() + ": "
                                    + BallerinaAzureClient.describe(e), e);
                }
                AzureFilesMetricsUtil.reportBytesTransferred(BallerinaAzureClient.getRemoteUrl(self),
                        BallerinaAzureClient.getProtocol(self), AzureFilesMetricsUtil.CONTEXT_CLIENT,
                        AzureFilesMetricsUtil.OPERATION_TYPE_GET, transferredBytes);
            } catch (UncheckedIOException e) {
                throw FilesErrorCreator.clientError(
                        "cannot write local file " + destinationPath.getValue() + ": "
                                + BallerinaAzureClient.describe(e.getCause()),
                        e);
            }
            return null;
        });
        return AzureFilesTracingUtil.sendTraces(result, env);
    }

    /**
     * Adjusts an inclusive {@code Range} for {@code downloadToFileWithResponse} alone. That call
     * splits the range into chunks with {@code pos < range.getEnd()}, treating the end bound as
     * exclusive, while every other range consumer (the {@code x-ms-range} header behind
     * {@code downloadWithResponse} and {@code openInputStream}) reads it as inclusive. Shifting
     * the end by one here keeps the connector's documented inclusive contract identical across
     * reads, and turns a single-byte range from zero chunks into one.
     *
     * @param range the inclusive range
     * @return the range this SDK call expects
     */
    private static ShareFileRange downloadToFileRange(ShareFileRange range) {
        return new ShareFileRange(range.getStart(), range.getEnd() + 1);
    }

    /** Opens the file's content stream and stores it on the Ballerina stream generator object. */
    public static Object openContentStream(Environment env, BObject self, BObject generator,
                                           BString path, Object options) {
        AzureFilesTracingUtil.sendMetricsData(env, BallerinaAzureClient.getRemoteUrl(self),
                BallerinaAzureClient.getProtocol(self), AzureFilesMetricsUtil.OPERATION_TYPE_GET,
                path.getValue());
        Object result = BallerinaAzureClient.invoke(env, () -> {
            OptionsReader.DownloadArgs args = OptionsReader.downloadArgs(options);
            ShareFileClient client = FileOps.fileClient(self, path, args.snapshotId());
            StorageFileInputStream stream = args.range() == null
                    ? client.openInputStream()
                    : client.openInputStream(OptionsReader.range(args.range()));
            generator.addNativeData(NATIVE_INPUT_STREAM, stream);
            generator.addNativeData(NATIVE_REMOTE_URL, BallerinaAzureClient.getRemoteUrl(self));
            generator.addNativeData(NATIVE_PROTOCOL, BallerinaAzureClient.getProtocol(self));
            return null;
        });
        return AzureFilesTracingUtil.sendTraces(result, env);
    }

    /** Reads the next chunk from an open content stream; {@code null} signals the end. */
    public static Object nextContentChunk(Environment env, BObject generator) {
        return BallerinaAzureClient.invoke(env, () -> {
            StorageFileInputStream stream =
                    (StorageFileInputStream) generator.getNativeData(NATIVE_INPUT_STREAM);
            if (stream == null) {
                return null;
            }
            try {
                byte[] buffer = new byte[READ_CHUNK_BYTES];
                int read = stream.read(buffer);
                if (read < 0) {
                    closeQuietly(generator);
                    return null;
                }
                byte[] chunk = read == buffer.length ? buffer : Arrays.copyOf(buffer, read);
                AzureFilesMetricsUtil.reportBytesTransferred((String) generator.getNativeData(NATIVE_REMOTE_URL),
                        (String) generator.getNativeData(NATIVE_PROTOCOL),
                        AzureFilesMetricsUtil.CONTEXT_CLIENT, AzureFilesMetricsUtil.OPERATION_TYPE_GET, read);
                return ValueCreator.createArrayValue(chunk);
            } catch (IOException | RuntimeException e) {
                // A service failure on a chunk read arrives wrapped in a RuntimeException; catching
                // IOException alone skipped both the close and the typed mapping.
                closeQuietly(generator);
                throw BallerinaAzureClient.mapFailure(e);
            }
        });
    }

    /** Closes an open content stream early. */
    public static Object closeContentStream(BObject generator) {
        closeQuietly(generator);
        return null;
    }

    static void closeQuietly(BObject generator) {
        StorageFileInputStream stream = (StorageFileInputStream) generator.getNativeData(NATIVE_INPUT_STREAM);
        if (stream != null) {
            generator.addNativeData(NATIVE_INPUT_STREAM, null);
            stream.close();
        }
    }

    private static byte[] contentBytes(Object content) {
        if (content instanceof BArray array) {
            // A byte-element array is raw content; every other list value is the string[][]
            // CSV rows the Ballerina-side record serialization produces. An empty array
            // serializes to a zero-byte file on either branch.
            if (TypeUtils.getReferredType(array.getType()) instanceof ArrayType arrayType
                    && TypeUtils.getReferredType(arrayType.getElementType()).getTag() == TypeTags.BYTE_TAG) {
                return array.getBytes();
            }
            return csvBytes(array);
        }
        if (content instanceof BString string) {
            return string.getValue().getBytes(StandardCharsets.UTF_8);
        }
        return ((BXml) content).toString().getBytes(StandardCharsets.UTF_8);
    }

    // Serializes CSV rows in the dialect data.csv reads by default: quote on comma, quote,
    // backslash, or line break, with backslash escaping (not RFC 4180 quote doubling); no
    // trailing newline, so an empty outer array yields a zero-byte file.
    private static byte[] csvBytes(BArray rows) {
        StringBuilder csv = new StringBuilder();
        for (int i = 0; i < rows.size(); i++) {
            if (i > 0) {
                csv.append('\n');
            }
            BArray row = (BArray) rows.get(i);
            for (int j = 0; j < row.size(); j++) {
                if (j > 0) {
                    csv.append(',');
                }
                appendCsvField(csv, ((BString) row.get(j)).getValue());
            }
        }
        return csv.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static void appendCsvField(StringBuilder csv, String field) {
        if (field.indexOf(',') < 0 && field.indexOf('"') < 0 && field.indexOf('\\') < 0
                && field.indexOf('\n') < 0 && field.indexOf('\r') < 0) {
            csv.append(field);
            return;
        }
        csv.append('"').append(field.replace("\\", "\\\\").replace("\"", "\\\"")).append('"');
    }
}
