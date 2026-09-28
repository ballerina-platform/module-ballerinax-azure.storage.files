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
import io.ballerina.lib.azure.storage.files.observability.AzureFilesMetricsUtil;
import io.ballerina.lib.azure.storage.files.observability.AzureFilesTracingUtil;
import io.ballerina.lib.azure.storage.files.util.BallerinaAzureClient;
import io.ballerina.lib.azure.storage.files.util.ContentBinder;
import io.ballerina.lib.azure.storage.files.util.DataBindingOptions;
import io.ballerina.lib.azure.storage.files.util.FilesErrorCreator;
import io.ballerina.lib.azure.storage.files.util.ModuleUtils;
import io.ballerina.lib.azure.storage.files.util.OptionsReader;
import io.ballerina.runtime.api.Environment;
import io.ballerina.runtime.api.creators.TypeCreator;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.types.ArrayType;
import io.ballerina.runtime.api.types.PredefinedTypes;
import io.ballerina.runtime.api.types.StreamType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.types.TypeTags;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BStream;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.api.values.BTypedesc;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.util.Locale;

/**
 * The {@code getFile} content retrieval: downloads a file and hands it back in the form the
 * caller-directed target type selects (raw bytes, text, a JSON or XML value, a record shape
 * bound per the resolved format, a lazy byte stream, or a lazy stream of CSV-bound records).
 * Binding is strict and runs through the same data.jsondata/xmldata/csv modules the
 * listener uses.
 */
public final class TypedReadOps {

    private static final String XML_TYPE_NAME = "xml";
    // The generator class backing the lazy byte stream, declared in natives.bal.
    private static final String CONTENT_STREAM_GENERATOR_CLASS = "ContentStreamGenerator";

    private static final String FORMAT_JSON = "JSON";
    private static final String FORMAT_XML = "XML";
    private static final String FORMAT_CSV = "CSV";

    private static final String JSON_BIND_CONTEXT = "the file content does not bind to the target JSON type";
    private static final String XML_BIND_CONTEXT = "the file content does not bind to the target XML type";
    private static final String XML_PARSE_CONTEXT = "the file content is not valid XML";
    private static final String CSV_BIND_CONTEXT = "the file content does not bind to the target CSV type";

    private TypedReadOps() {
    }

    /** Retrieves the file's content in the form the target typedesc selects, for Client and Caller. */
    public static Object getFile(Environment env, BObject self, BString path, Object options,
                                 BTypedesc targetType) {
        // A Caller carries no native data; unwrap it to the Client it holds.
        BObject clientObj = self.getNativeData(BallerinaAzureClient.NATIVE_SHARE_CLIENT) == null
                ? (BObject) self.getObjectValue(BallerinaAzureClient.CALLER_CLIENT_FIELD) : self;
        AzureFilesTracingUtil.sendMetricsData(env, BallerinaAzureClient.getRemoteUrl(clientObj),
                BallerinaAzureClient.getProtocol(clientObj), AzureFilesMetricsUtil.OPERATION_TYPE_GET,
                path.getValue());
        Type described = TypeUtils.getReferredType(targetType.getDescribingType());
        if (described.getTag() == TypeTags.STREAM_TAG) {
            Object result = streamTarget(env, clientObj, path, options, (StreamType) described);
            return AzureFilesTracingUtil.sendTraces(result, env);
        }
        Object bytes = readFileBytes(env, clientObj, path, options);
        if (bytes instanceof BError) {
            return AzureFilesTracingUtil.sendTraces(bytes, env);
        }
        Object result = bindMaterialized(env, (BArray) bytes, described, path, options);
        return AzureFilesTracingUtil.sendTraces(result, env);
    }

    // Binds materialized content to a non-stream target.
    private static Object bindMaterialized(Environment env, BArray byteArray, Type described,
                                           BString path, Object options) {
        switch (described.getTag()) {
            case TypeTags.STRING_TAG:
                return decodeText(byteArray);
            case TypeTags.ARRAY_TAG:
                return bindArrayTarget(env, byteArray, (ArrayType) described, path, options);
            case TypeTags.RECORD_TYPE_TAG:
            case TypeTags.MAP_TAG:
                return bindRecordTarget(byteArray, described, path, options);
            default:
                if (XML_TYPE_NAME.equals(described.getQualifiedName())) {
                    return bindXml(byteArray, described);
                }
                // json (and json-shaped unions) bind through the JSON parser.
                return bindJson(byteArray, described);
        }
    }

    // byte[] is the raw content; a record or map array binds per the resolved format (a
    // JSON array or CSV rows; XML has no top-level array). Any other array is a json-shaped
    // target: it binds through the JSON parser, except that CSV content never binds to it.
    private static Object bindArrayTarget(Environment env, BArray byteArray, ArrayType arrayType,
                                          BString path, Object options) {
        Type element = TypeUtils.getReferredType(arrayType.getElementType());
        if (element.getTag() == TypeTags.BYTE_TAG) {
            return byteArray;
        }
        String format = resolveFormat(path, options);
        if (element.getTag() != TypeTags.RECORD_TYPE_TAG && element.getTag() != TypeTags.MAP_TAG) {
            if (FORMAT_CSV.equals(format)) {
                return FilesErrorCreator.clientError(
                        "CSV content binds to a record array target; read the content as string "
                                + "or byte[] and bind rows with the data.csv module", null);
            }
            return bindJson(byteArray, arrayType);
        }
        if (format == null) {
            return unresolvableFormat("record array");
        }
        return switch (format) {
            case FORMAT_JSON -> bindJson(byteArray, arrayType);
            case FORMAT_CSV -> parseCsv(env, byteArray, arrayType);
            default -> FilesErrorCreator.clientError(
                    "a record array target does not bind from XML; use a '.json' or '.csv' source, "
                            + "or an explicit fileFormat", null);
        };
    }

    // A single record (or map) binds per the resolved format; a record is never CSV.
    private static Object bindRecordTarget(BArray byteArray, Type described, BString path, Object options) {
        String format = resolveFormat(path, options);
        if (format == null) {
            return unresolvableFormat("record");
        }
        return switch (format) {
            case FORMAT_JSON -> bindJson(byteArray, described);
            case FORMAT_XML -> bindXml(byteArray, described);
            default -> FilesErrorCreator.clientError(
                    "a record target does not bind from CSV; use a record array target for CSV rows", null);
        };
    }

    // A stream target reads lazily: byte streams pull chunks from the open content stream,
    // record streams bind CSV rows through data.csv's own stream parser.
    private static Object streamTarget(Environment env, BObject clientObj, BString path, Object options,
                                       StreamType streamType) {
        BObject generator = ValueCreator.createObjectValue(ModuleUtils.getModule(), CONTENT_STREAM_GENERATOR_CLASS);
        Object opened = TransferOps.openContentStream(env, clientObj, generator, path, options);
        if (opened instanceof BError) {
            return opened;
        }
        Type constraint = TypeUtils.getReferredType(streamType.getConstrainedType());
        boolean byteStream = constraint.getTag() == TypeTags.ARRAY_TAG
                && TypeUtils.getReferredType(((ArrayType) constraint).getElementType()).getTag()
                        == TypeTags.BYTE_TAG;
        if (byteStream) {
            // The stream is typed with the DECLARED constraint and completion, so a target
            // narrowed to the module's Error completion casts cleanly.
            return ValueCreator.createStreamValue(
                    TypeCreator.createStreamType(constraint, streamType.getCompletionType()), generator);
        }
        BStream byteFeed = ValueCreator.createStreamValue(TypeCreator.createStreamType(
                TypeCreator.createArrayType(PredefinedTypes.TYPE_BYTE), completionType()), generator);
        Object rows = io.ballerina.lib.data.csvdata.csv.Native.parseToStream(env, byteFeed,
                DataBindingOptions.csvParseOptions(false),
                ValueCreator.createTypedescValue(constraint));
        if (rows instanceof BError bError) {
            // The generator already holds an open source stream; the listener's twin of this
            // branch releases it, and so must this one.
            TransferOps.closeQuietly(generator);
            return csvFailure(bError);
        }
        return rows;
    }

    // Downloads the file's full content (or a range of it) into a Ballerina byte array.
    private static Object readFileBytes(Environment env, BObject clientObj, BString path, Object options) {
        return BallerinaAzureClient.invoke(env, () -> {
            OptionsReader.DownloadArgs args = OptionsReader.downloadArgs(options);
            ShareFileClient client = FileOps.fileClient(clientObj, path, args.snapshotId());
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            if (args.range() == null) {
                client.download(out);
            } else {
                client.downloadWithResponse(out, OptionsReader.range(args.range()), null, null, null);
            }
            byte[] bytes = out.toByteArray();
            AzureFilesMetricsUtil.reportBytesTransferred(BallerinaAzureClient.getRemoteUrl(clientObj),
                    BallerinaAzureClient.getProtocol(clientObj), AzureFilesMetricsUtil.CONTEXT_CLIENT,
                    AzureFilesMetricsUtil.OPERATION_TYPE_GET, bytes.length);
            return ValueCreator.createArrayValue(bytes);
        });
    }

    private static Object bindJson(BArray content, Type target) {
        try {
            return ContentBinder.bindJson(content, target, false, JSON_BIND_CONTEXT);
        } catch (BError e) {
            return e;
        }
    }

    private static Object bindXml(BArray content, Type target) {
        try {
            return ContentBinder.bindXml(content, target, false, XML_BIND_CONTEXT, XML_PARSE_CONTEXT);
        } catch (BError e) {
            return e;
        }
    }

    private static Object parseCsv(Environment env, BArray byteArray, Type target) {
        try {
            Object result = io.ballerina.lib.data.csvdata.csv.Native.parseBytes(env, byteArray,
                    DataBindingOptions.csvParseOptions(false),
                    ValueCreator.createTypedescValue(target));
            return result instanceof BError bError ? csvFailure(bError) : result;
        } catch (BError e) {
            return csvFailure(e);
        }
    }

    private static Object decodeText(BArray byteArray) {
        try {
            String text = StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(byteArray.getBytes())).toString();
            return StringUtils.fromString(text);
        } catch (CharacterCodingException e) {
            return FilesErrorCreator.clientError(
                    "the file content is not valid UTF-8 text: " + BallerinaAzureClient.describe(e), e);
        }
    }

    // Resolves the binding format of a record-shaped target: the explicit fileFormat
    // override wins, else the path's extension decides; null means unresolvable.
    private static String resolveFormat(BString path, Object options) {
        if (options != null) {
            @SuppressWarnings("unchecked")
            BMap<BString, Object> record = (BMap<BString, Object>) options;
            BString override = record.getStringValue(OptionsReader.FILE_FORMAT);
            if (override != null) {
                return override.getValue();
            }
        }
        String lower = path.getValue().toLowerCase(Locale.ROOT);
        if (lower.endsWith(".json")) {
            return FORMAT_JSON;
        }
        if (lower.endsWith(".xml")) {
            return FORMAT_XML;
        }
        if (lower.endsWith(".csv")) {
            return FORMAT_CSV;
        }
        return null;
    }

    private static Type completionType() {
        return TypeCreator.createUnionType(PredefinedTypes.TYPE_ERROR, PredefinedTypes.TYPE_NULL);
    }

    private static BError unresolvableFormat(String targetKind) {
        return FilesErrorCreator.clientError("a " + targetKind + " target requires a '.json', '.xml', or "
                + "'.csv' extension in the path, or an explicit fileFormat", null);
    }

    private static BError csvFailure(BError cause) {
        return FilesErrorCreator.clientError(CSV_BIND_CONTEXT + ": " + cause.getErrorMessage(), cause);
    }
}
