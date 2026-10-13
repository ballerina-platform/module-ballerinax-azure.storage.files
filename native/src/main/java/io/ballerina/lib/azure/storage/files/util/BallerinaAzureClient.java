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

package io.ballerina.lib.azure.storage.files.util;

import com.azure.core.http.HttpPipeline;
import com.azure.core.http.HttpPipelineBuilder;
import com.azure.core.http.policy.HttpPipelinePolicy;
import com.azure.core.util.UrlBuilder;
import com.azure.storage.file.share.ShareClient;
import com.azure.storage.file.share.ShareClientBuilder;
import com.azure.storage.file.share.ShareServiceClient;
import com.azure.storage.file.share.models.ShareStorageException;
import io.ballerina.runtime.api.Environment;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * Shared plumbing for the native operations: runs the blocking SDK call off the Ballerina
 * scheduler via {@link Environment#yieldAndRun}, converts every failure to a typed Ballerina
 * error, and fetches the SDK clients stored on the Ballerina client objects.
 */
public final class BallerinaAzureClient {

    // Keys under which the SDK clients are stored on client objects.
    public static final String NATIVE_SERVICE_CLIENT = "serviceClient";
    public static final String NATIVE_SHARE_CLIENT = "shareClient";
    /** The Caller field holding the wrapped Client object. */
    public static final BString CALLER_CLIENT_FIELD = StringUtils.fromString("client");
    public static final String NATIVE_REMOTE_URL = "remoteUrl";
    public static final String NATIVE_PROTOCOL = "protocol";
    // The query parameter that addresses a share snapshot on the wire.
    private static final String SHARE_SNAPSHOT_PARAM = "sharesnapshot";

    private BallerinaAzureClient() {
    }

    /**
     * Runs a blocking operation body and maps its outcome to a Ballerina value.
     *
     * @param env  the Ballerina runtime environment
     * @param body the operation body; its return value is passed through verbatim
     * @return the body's result, or the mapped Ballerina error on failure
     */
    public static Object invoke(Environment env, Supplier<Object> body) {
        return env.yieldAndRun(() -> {
            try {
                return body.get();
            } catch (Exception e) {
                return mapFailure(e);
            }
        });
    }

    private static final int MAX_CAUSE_DEPTH = 8;

    /**
     * Maps a failure to the module's typed error: Azure service failures go through the
     * code-keyed mapper, Ballerina errors pass through, and anything else becomes the
     * generic client-side {@code Error}.
     *
     * @param e the failure
     * @return the mapped Ballerina error
     */
    public static BError mapFailure(Throwable e) {
        if (e instanceof BError bError) {
            return bError;
        }
        // A service failure raised mid-stream reaches us wrapped: the SDK's input stream rethrows
        // it inside a RuntimeException. Its origin is still the service, so the hierarchy's
        // origin rule says it must keep its status and error code rather than collapse to the
        // generic client-side Error. Bounded and cycle-guarded, since a cause chain can loop.
        Throwable current = e;
        for (int depth = 0; current != null && depth < MAX_CAUSE_DEPTH; depth++) {
            if (current instanceof ShareStorageException storageException) {
                return ErrorMapper.toBError(storageException);
            }
            Throwable cause = current.getCause();
            current = cause == current ? null : cause;
        }
        return FilesErrorCreator.clientError(describe(e), e);
    }

    /** Builds a human-readable message for an unexpected local exception. */
    public static String describe(Throwable t) {
        return t.getMessage() == null ? t.getClass().getSimpleName() : t.getMessage();
    }

    /**
     * Returns the {@code ShareServiceClient} stored on a client object.
     *
     * @param self the Ballerina client object
     * @return the SDK service client
     */
    public static ShareServiceClient getServiceClient(BObject self) {
        return (ShareServiceClient) self.getNativeData(NATIVE_SERVICE_CLIENT);
    }

    /**
     * Returns the {@code ShareClient} stored on a share-bound client object.
     *
     * @param self the Ballerina client object
     * @return the SDK share client
     */
    public static ShareClient getShareClient(BObject self) {
        return (ShareClient) self.getNativeData(NATIVE_SHARE_CLIENT);
    }

    /**
     * Returns the {@code ShareClient} for the live share, or for one of its snapshots when a
     * snapshot id is given.
     *
     * <p>The snapshot client is rebuilt with an extra pipeline policy that appends the
     * {@code sharesnapshot} query parameter to any request missing it. The SDK's download path
     * hard-codes that parameter to {@code null}, so without the policy every content read from
     * a snapshot client silently serves the live file.
     *
     * @param self       the Ballerina client object
     * @param snapshotId the snapshot to read from, or {@code null} for the live share
     * @return the SDK share client
     */
    public static ShareClient getShareClient(BObject self, String snapshotId) {
        ShareClient base = getShareClient(self);
        if (snapshotId == null) {
            return base;
        }
        String encodedId = URLEncoder.encode(snapshotId, StandardCharsets.UTF_8);
        HttpPipelinePolicy ensureSnapshotParam = (context, next) -> {
            UrlBuilder url = UrlBuilder.parse(context.getHttpRequest().getUrl());
            if (!url.getQuery().containsKey(SHARE_SNAPSHOT_PARAM)) {
                url.setQueryParameter(SHARE_SNAPSHOT_PARAM, encodedId);
                context.getHttpRequest().setUrl(url.toString());
            }
            return next.process();
        };
        HttpPipeline pipeline = base.getHttpPipeline();
        // The parameter must be on the URL before the credential policy signs the request
        // (shared-key signatures cover the canonicalized query), so the policy is inserted
        // ahead of the first credential policy rather than appended.
        List<HttpPipelinePolicy> policies = new ArrayList<>();
        int insertAt = -1;
        for (int i = 0; i < pipeline.getPolicyCount(); i++) {
            HttpPipelinePolicy policy = pipeline.getPolicy(i);
            if (insertAt == -1 && policy.getClass().getSimpleName().contains("Credential")) {
                insertAt = i;
            }
            policies.add(policy);
        }
        policies.add(insertAt == -1 ? policies.size() : insertAt, ensureSnapshotParam);
        return new ShareClientBuilder()
                .pipeline(new HttpPipelineBuilder()
                        .policies(policies.toArray(new HttpPipelinePolicy[0]))
                        .httpClient(pipeline.getHttpClient())
                        .build())
                .endpoint(base.getAccountUrl())
                .shareName(base.getShareName())
                .snapshot(snapshotId)
                .buildClient();
    }

    /**
     * Normalizes a share-relative path for the SDK: strips the leading slash and rejects an
     * empty result.
     *
     * @param path the combined slash-delimited path
     * @return the SDK-form path, relative to the share root without a leading slash
     */
    public static String filePath(BString path) {
        String p = trimSlashes(path.getValue());
        if (p.isEmpty()) {
            throw FilesErrorCreator.clientError("the path must name a file, not the share root", null);
        }
        return p;
    }

    /**
     * Normalizes a directory path for the SDK. An empty path or {@code /} addresses the share
     * root directory.
     *
     * @param path the combined slash-delimited path
     * @return the SDK-form path; empty string for the share root
     */
    public static String directoryPath(BString path) {
        return trimSlashes(path.getValue());
    }

    private static String trimSlashes(String p) {
        String result = p.strip();
        while (result.startsWith("/")) {
            result = result.substring(1);
        }
        while (result.endsWith("/")) {
            result = result.substring(0, result.length() - 1);
        }
        return result;
    }

    /**
     * Returns the remote URL stored on a client object for observability.
     *
     * @param self the Ballerina client object
     * @return the remote URL, or {@code null} if not set
     */
    public static String getRemoteUrl(BObject self) {
        return (String) self.getNativeData(NATIVE_REMOTE_URL);
    }

    /**
     * Returns the wire protocol stored on a client object for observability.
     *
     * @param self the Ballerina client object
     * @return the protocol (e.g. "https"), or {@code null} if not set
     */
    public static String getProtocol(BObject self) {
        return (String) self.getNativeData(NATIVE_PROTOCOL);
    }

    /**
     * Extracts the host (with port if non-default) from an account URL for the {@code remote.url} tag.
     *
     * @param accountUrl the Azure account URL (e.g. "https://myaccount.file.core.windows.net")
     * @return the host:port string, or the raw URL if parsing fails
     */
    public static String extractHost(String accountUrl) {
        if (accountUrl == null) {
            return null;
        }
        try {
            java.net.URI uri = new java.net.URI(accountUrl);
            String host = uri.getHost();
            int port = uri.getPort();
            return port > 0 ? host + ":" + port : host;
        } catch (java.net.URISyntaxException e) {
            return accountUrl;
        }
    }

    /**
     * Extracts the protocol scheme from an account URL.
     *
     * @param accountUrl the Azure account URL
     * @return the scheme (e.g. "https"), or "https" if parsing fails
     */
    public static String extractProtocol(String accountUrl) {
        if (accountUrl == null) {
            return "https";
        }
        try {
            java.net.URI uri = new java.net.URI(accountUrl);
            String scheme = uri.getScheme();
            return scheme != null ? scheme : "https";
        } catch (java.net.URISyntaxException e) {
            return "https";
        }
    }
}
