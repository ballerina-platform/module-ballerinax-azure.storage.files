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

package io.ballerina.lib.azure.storage.files.observability;

import io.ballerina.runtime.api.Environment;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.observability.ObservabilityConstants;
import io.ballerina.runtime.observability.ObserveUtils;
import io.ballerina.runtime.observability.ObserverContext;
import io.ballerina.runtime.observability.tracer.BSpan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for injecting Azure Files observability context into Ballerina strands and spans.
 *
 * <p>Two usage patterns:
 * <ul>
 *   <li><b>Listener dispatch</b> — call {@link #createStrandProperties} to build a properties map
 *       embedded in {@code StrandMetadata} for service method dispatch.</li>
 *   <li><b>Client operations</b> — call {@link #sendMetricsData} from inside a native external
 *       method to enrich the auto-instrumented span with Azure Files-specific tags.</li>
 * </ul>
 */
public final class AzureFilesTracingUtil {

    private static final Logger log = LoggerFactory.getLogger(AzureFilesTracingUtil.class);

    private AzureFilesTracingUtil() {
    }

    /**
     * Creates a per-file parent span that covers the entire file lifecycle (found -> cleaned_up).
     *
     * @param url      remote URL
     * @param protocol wire protocol
     * @param filePath path of the file being processed (added as a trace-only tag)
     * @return context with parent span, or {@code null} if observability is disabled
     */
    public static AzureFilesObserverContext createFileLifecycleContext(String url, String protocol, String filePath) {
        if (!ObserveUtils.isObservabilityEnabled()) {
            return null;
        }
        try {
            AzureFilesObserverContext ctx = new AzureFilesObserverContext(
                    AzureFilesMetricsUtil.CONTEXT_LISTENER, url, protocol);
            ctx.addTag(AzureFilesObserverContext.TAG_ACTION_TYPE, AzureFilesMetricsUtil.ACTION_TYPE_EVENT);
            String instanceUrl = AzureFilesMetricsUtil.getInstanceUrl();
            if (instanceUrl != null) {
                ctx.addTag(AzureFilesObserverContext.TAG_INSTANCE_URL, instanceUrl);
            }
            BSpan span = BSpan.start("azure_files", "file-lifecycle", false);
            span.addTag(AzureFilesObserverContext.TAG_MODULE, AzureFilesMetricsUtil.MODULE_AZURE_FILES);
            span.addTag(AzureFilesObserverContext.TAG_CONTEXT, AzureFilesMetricsUtil.CONTEXT_LISTENER);
            span.addTag(AzureFilesObserverContext.TAG_REMOTE_URL, url != null ? url : AzureFilesMetricsUtil.UNKNOWN);
            span.addTag(AzureFilesObserverContext.TAG_PROTOCOL,
                    protocol != null ? protocol : AzureFilesMetricsUtil.UNKNOWN);
            span.addTag(AzureFilesObserverContext.TAG_ACTION_TYPE, AzureFilesMetricsUtil.ACTION_TYPE_EVENT);
            if (filePath != null) {
                span.addTag(AzureFilesObserverContext.TAG_FILE_PATH, filePath);
            }
            ctx.setSpan(span);
            return ctx;
        } catch (Throwable t) {
            log.debug("Failed to create file lifecycle context", t);
            return null;
        }
    }

    /**
     * Sets a parent context on the observer context inside strand properties.
     *
     * @param strandProperties the strand properties map (may be null)
     * @param parentCtx        the parent context with a span set on it (may be null)
     */
    public static void setParentContext(Map<String, Object> strandProperties,
                                         AzureFilesObserverContext parentCtx) {
        if (strandProperties == null || parentCtx == null) {
            return;
        }
        try {
            Object ctxObj = strandProperties.get(ObservabilityConstants.KEY_OBSERVER_CONTEXT);
            if (ctxObj instanceof ObserverContext ctx) {
                ctx.setParent(parentCtx);
            }
        } catch (Throwable t) {
            log.debug("Failed to set parent context on strand properties", t);
        }
    }

    /**
     * Finishes the per-file parent span.
     *
     * @param parentCtx the parent context returned by {@link #createFileLifecycleContext}, or null
     */
    public static void finishFileLifecycleSpan(AzureFilesObserverContext parentCtx) {
        if (parentCtx == null) {
            return;
        }
        try {
            BSpan span = parentCtx.getSpan();
            if (span != null) {
                span.finishSpan();
            }
        } catch (Throwable t) {
            log.debug("Failed to finish file lifecycle span", t);
        }
    }

    public static BSpan createChildSpan(AzureFilesObserverContext parentCtx, String operation,
                                        String fileStage, String cleanupAction, String handlerName) {
        if (!ObserveUtils.isObservabilityEnabled() || parentCtx == null || parentCtx.getSpan() == null) {
            return null;
        }
        try {
            BSpan span = BSpan.start(parentCtx.getSpan(), "azure_files", operation, false);
            span.addTag(AzureFilesObserverContext.TAG_MODULE, AzureFilesMetricsUtil.MODULE_AZURE_FILES);
            span.addTag(AzureFilesObserverContext.TAG_CONTEXT, AzureFilesMetricsUtil.CONTEXT_LISTENER);
            span.addTag(AzureFilesObserverContext.TAG_ACTION_TYPE, AzureFilesMetricsUtil.ACTION_TYPE_EVENT);
            copyTag(parentCtx, span, AzureFilesObserverContext.TAG_REMOTE_URL);
            copyTag(parentCtx, span, AzureFilesObserverContext.TAG_PROTOCOL);
            span.addTag(AzureFilesObserverContext.TAG_FILE_STAGE,
                    fileStage != null ? fileStage : AzureFilesMetricsUtil.NONE);
            span.addTag(AzureFilesObserverContext.TAG_CLEANUP_ACTION,
                    cleanupAction != null ? cleanupAction : AzureFilesMetricsUtil.NONE);
            span.addTag(AzureFilesObserverContext.TAG_HANDLER_NAME,
                    handlerName != null ? handlerName : AzureFilesMetricsUtil.NONE);
            return span;
        } catch (Throwable t) {
            log.debug("Failed to create child span", t);
            return null;
        }
    }

    private static void copyTag(AzureFilesObserverContext source, BSpan target, String key) {
        if (source.getTag(key) != null) {
            target.addTag(key, source.getTag(key).getValue());
        }
    }

    public static void addOutcomeToSpan(BSpan span, String outcome, String errorType) {
        if (span == null) {
            return;
        }
        span.addTag(AzureFilesObserverContext.TAG_OUTCOME, outcome);
        if (errorType != null) {
            span.addTag(AzureFilesObserverContext.TAG_ERROR_TYPE, errorType);
            span.addTag(AzureFilesObserverContext.TAG_FAILURE_REASON, errorType);
        }
    }

    /**
     * Creates strand properties containing an {@link AzureFilesObserverContext} for a listener event
     * dispatch.
     *
     * @param context   context tag value
     * @param url       remote URL
     * @param protocol  wire protocol
     * @param eventType event type tag value (e.g. {@link AzureFilesMetricsUtil#EVENT_TYPE_CHANGE})
     * @param filePath  retained for API compatibility; not added to the context
     * @return properties map, or {@code null} if observability is disabled
     */
    public static Map<String, Object> createStrandProperties(String context, String url, String protocol,
                                                              String eventType, String filePath) {
        if (!ObserveUtils.isObservabilityEnabled()) {
            return null;
        }
        try {
            AzureFilesObserverContext observerContext = new AzureFilesObserverContext(context, url, protocol);
            observerContext.addTag(AzureFilesObserverContext.TAG_ACTION_TYPE, AzureFilesMetricsUtil.ACTION_TYPE_EVENT);
            String instanceUrl = AzureFilesMetricsUtil.getInstanceUrl();
            if (instanceUrl != null) {
                observerContext.addTag(AzureFilesObserverContext.TAG_INSTANCE_URL, instanceUrl);
            }
            observerContext.addTag(AzureFilesObserverContext.TAG_EVENT_TYPE, eventType);
            Map<String, Object> properties = new HashMap<>();
            properties.put(ObservabilityConstants.KEY_OBSERVER_CONTEXT, observerContext);
            return properties;
        } catch (Throwable t) {
            log.debug("Failed to create strand properties", t);
            return null;
        }
    }

    /**
     * Overload without {@code filePath}.
     */
    public static Map<String, Object> createStrandProperties(String context, String url, String protocol,
                                                              String eventType) {
        return createStrandProperties(context, url, protocol, eventType, null);
    }

    /**
     * Creates strand properties for a listener file lifecycle event with file stage, handler name,
     * and file metadata.
     *
     * @param context      context tag
     * @param url          remote URL
     * @param protocol     wire protocol
     * @param eventType    event type (e.g. "create")
     * @param fileStage    file lifecycle stage
     * @param handlerName  handler method name, or {@code null}
     * @param fileSize     file size in bytes, or -1 if unknown
     * @param modifiedTime last-modified timestamp, or -1 if unknown
     * @return properties map, or {@code null} if observability is disabled
     */
    public static Map<String, Object> createFileStageStrandProperties(String context, String url, String protocol,
                                                                       String eventType, String fileStage,
                                                                       String handlerName, long fileSize,
                                                                       long modifiedTime) {
        if (!ObserveUtils.isObservabilityEnabled()) {
            return null;
        }
        try {
            AzureFilesObserverContext observerContext = new AzureFilesObserverContext(context, url, protocol);
            observerContext.addTag(AzureFilesObserverContext.TAG_ACTION_TYPE, AzureFilesMetricsUtil.ACTION_TYPE_EVENT);
            String instanceUrl = AzureFilesMetricsUtil.getInstanceUrl();
            if (instanceUrl != null) {
                observerContext.addTag(AzureFilesObserverContext.TAG_INSTANCE_URL, instanceUrl);
            }
            observerContext.addTag(AzureFilesObserverContext.TAG_EVENT_TYPE, eventType);
            observerContext.addTag(AzureFilesObserverContext.TAG_FILE_STAGE, fileStage);
            if (handlerName != null) {
                observerContext.addTag(AzureFilesObserverContext.TAG_HANDLER_NAME, handlerName);
            }
            if (fileSize >= 0) {
                observerContext.addProperty(AzureFilesObserverContext.TAG_FILE_SIZE, fileSize);
            }
            if (modifiedTime >= 0) {
                observerContext.addProperty(AzureFilesObserverContext.TAG_FILE_MODIFIED_TIME, modifiedTime);
            }
            Map<String, Object> properties = new HashMap<>();
            properties.put(ObservabilityConstants.KEY_OBSERVER_CONTEXT, observerContext);
            return properties;
        } catch (Throwable t) {
            log.debug("Failed to create file stage strand properties", t);
            return null;
        }
    }

    /**
     * Creates strand properties for a cleanup (post-processing) span.
     *
     * @param context       context tag
     * @param url           remote URL
     * @param protocol      wire protocol
     * @param cleanupAction cleanup action type (move, delete, none)
     * @param handlerName   handler method name that triggered this cleanup
     * @return properties map, or {@code null} if observability is disabled
     */
    public static Map<String, Object> createCleanupStrandProperties(String context, String url, String protocol,
                                                                     String cleanupAction, String handlerName) {
        if (!ObserveUtils.isObservabilityEnabled()) {
            return null;
        }
        try {
            AzureFilesObserverContext observerContext = new AzureFilesObserverContext(context, url, protocol);
            observerContext.addTag(AzureFilesObserverContext.TAG_ACTION_TYPE, AzureFilesMetricsUtil.ACTION_TYPE_EVENT);
            String instanceUrl = AzureFilesMetricsUtil.getInstanceUrl();
            if (instanceUrl != null) {
                observerContext.addTag(AzureFilesObserverContext.TAG_INSTANCE_URL, instanceUrl);
            }
            observerContext.addTag(AzureFilesObserverContext.TAG_FILE_STAGE,
                    AzureFilesMetricsUtil.FILE_STAGE_CLEANED_UP);
            observerContext.addTag(AzureFilesObserverContext.TAG_CLEANUP_ACTION, cleanupAction);
            if (handlerName != null) {
                observerContext.addTag(AzureFilesObserverContext.TAG_HANDLER_NAME, handlerName);
            }
            Map<String, Object> properties = new HashMap<>();
            properties.put(ObservabilityConstants.KEY_OBSERVER_CONTEXT, observerContext);
            return properties;
        } catch (Throwable t) {
            log.debug("Failed to create cleanup strand properties", t);
            return null;
        }
    }

    /**
     * Creates strand properties for a listener error dispatch.
     *
     * @param context   context tag
     * @param url       remote URL
     * @param protocol  wire protocol
     * @param filePath  path of the file involved, or {@code null}
     * @param errorType Ballerina error type name
     * @return properties map, or {@code null} if observability is disabled
     */
    public static Map<String, Object> createErrorStrandProperties(String context, String url, String protocol,
                                                                   String filePath, String errorType) {
        try {
            Map<String, Object> props = createStrandProperties(context, url, protocol,
                    AzureFilesMetricsUtil.EVENT_TYPE_ERROR, filePath);
            if (props != null) {
                AzureFilesObserverContext ctx = (AzureFilesObserverContext) props.get(
                        ObservabilityConstants.KEY_OBSERVER_CONTEXT);
                ctx.addTag(AzureFilesObserverContext.TAG_ERROR_TYPE, errorType);
                ctx.addTag(AzureFilesObserverContext.TAG_FAILURE_REASON, errorType);
                ctx.addTag(AzureFilesObserverContext.TAG_OUTCOME, AzureFilesMetricsUtil.OUTCOME_FAILURE);
            }
            return props;
        } catch (Throwable t) {
            log.debug("Failed to create error strand properties", t);
            return null;
        }
    }

    /**
     * Adds outcome and optional error type tags to strand properties.
     *
     * @param strandProperties the strand properties map (may be null)
     * @param outcome          success or failure
     * @param errorType        error type, or null
     */
    public static void addOutcomeToStrandProperties(Map<String, Object> strandProperties, String outcome,
                                                     String errorType) {
        if (strandProperties == null) {
            return;
        }
        try {
            AzureFilesObserverContext ctx = (AzureFilesObserverContext) strandProperties.get(
                    ObservabilityConstants.KEY_OBSERVER_CONTEXT);
            if (ctx == null) {
                return;
            }
            ctx.addTag(AzureFilesObserverContext.TAG_OUTCOME, outcome);
            if (errorType != null) {
                ctx.addTag(AzureFilesObserverContext.TAG_ERROR_TYPE, errorType);
            }
        } catch (Throwable t) {
            log.debug("Failed to add outcome to strand properties", t);
        }
    }

    /**
     * Enriches the auto-instrumented span for the currently executing native external method with
     * Azure Files client-operation tags.
     *
     * @param env           the current Ballerina environment
     * @param url           remote URL
     * @param protocol      wire protocol
     * @param operationType one of the {@code OPERATION_TYPE_*} constants
     * @param filePath      source/target file path
     */
    public static void sendMetricsData(Environment env, String url, String protocol,
                                       String operationType, String filePath) {
        sendMetricsData(env, url, protocol, operationType, filePath, null);
    }

    /**
     * Variant for two-path operations ({@code rename}, {@code move}, {@code copy}).
     *
     * @param env             the current Ballerina environment
     * @param url             remote URL
     * @param protocol        wire protocol
     * @param operationType   operation type constant
     * @param filePath        source path
     * @param destinationPath destination path, or {@code null}
     */
    public static void sendMetricsData(Environment env, String url, String protocol,
                                       String operationType, String filePath,
                                       String destinationPath) {
        try {
            ObserverContext ctx = ObserveUtils.getObserverContextOfCurrentFrame(env);
            if (ctx == null) {
                return;
            }
            ctx.addTag(AzureFilesObserverContext.TAG_MODULE, AzureFilesMetricsUtil.MODULE_AZURE_FILES);
            ctx.addTag(AzureFilesObserverContext.TAG_ACTION_TYPE, AzureFilesMetricsUtil.ACTION_TYPE_OPERATION);
            ctx.addTag(AzureFilesObserverContext.TAG_CONTEXT, AzureFilesMetricsUtil.CONTEXT_CLIENT);
            ctx.addTag(AzureFilesObserverContext.TAG_REMOTE_URL, url);
            ctx.addTag(AzureFilesObserverContext.TAG_PROTOCOL, protocol);
            ctx.addTag(AzureFilesObserverContext.TAG_OPERATION_TYPE, operationType);
            String instanceUrl = AzureFilesMetricsUtil.getInstanceUrl();
            if (instanceUrl != null) {
                ctx.addTag(AzureFilesObserverContext.TAG_INSTANCE_URL, instanceUrl);
            }
            BSpan span = ctx.getSpan();
            if (span != null) {
                if (filePath != null) {
                    span.addTag(AzureFilesObserverContext.TAG_FILE_PATH, filePath);
                }
                if (destinationPath != null) {
                    span.addTag(AzureFilesObserverContext.TAG_DESTINATION_PATH, destinationPath);
                }
            }
        } catch (Throwable t) {
            log.debug("Failed to send metrics data", t);
        }
    }

    /**
     * Tags the auto-instrumented span of the current frame as a poll cycle span.
     *
     * @param env      the current Ballerina environment
     * @param url      remote URL
     * @param protocol wire protocol
     */
    public static void sendPollMetricsData(Environment env, String url, String protocol) {
        try {
            ObserverContext ctx = ObserveUtils.getObserverContextOfCurrentFrame(env);
            if (ctx == null) {
                return;
            }
            ctx.addTag(AzureFilesObserverContext.TAG_MODULE, AzureFilesMetricsUtil.MODULE_AZURE_FILES);
            ctx.addTag(AzureFilesObserverContext.TAG_ACTION_TYPE, AzureFilesMetricsUtil.ACTION_TYPE_POLL);
            ctx.addTag(AzureFilesObserverContext.TAG_CONTEXT, AzureFilesMetricsUtil.CONTEXT_LISTENER);
            ctx.addTag(AzureFilesObserverContext.TAG_REMOTE_URL, url != null ? url : AzureFilesMetricsUtil.UNKNOWN);
            ctx.addTag(AzureFilesObserverContext.TAG_PROTOCOL,
                    protocol != null ? protocol : AzureFilesMetricsUtil.UNKNOWN);
            String instanceUrl = AzureFilesMetricsUtil.getInstanceUrl();
            if (instanceUrl != null) {
                ctx.addTag(AzureFilesObserverContext.TAG_INSTANCE_URL, instanceUrl);
            }
        } catch (Throwable t) {
            log.debug("Failed to send poll metrics data", t);
        }
    }

    /**
     * Adds the outcome tag to the poll span on the current frame.
     *
     * @param env     the current Ballerina environment
     * @param outcome success or failure
     */
    public static void sendPollOutcome(Environment env, String outcome) {
        try {
            ObserverContext ctx = ObserveUtils.getObserverContextOfCurrentFrame(env);
            if (ctx == null) {
                return;
            }
            ctx.addTag(AzureFilesObserverContext.TAG_OUTCOME, outcome);
        } catch (Throwable t) {
            log.debug("Failed to send poll outcome", t);
        }
    }

    /**
     * Adds {@code error=true} and {@code error.type} tags to the auto-instrumented span for the
     * currently executing native external method.
     *
     * @param env       the current Ballerina environment
     * @param errorType Ballerina error type name
     */
    public static void sendErrorMetricsOnCurrentFrame(Environment env, String errorType) {
        try {
            ObserverContext ctx = ObserveUtils.getObserverContextOfCurrentFrame(env);
            if (ctx == null) {
                return;
            }
            ctx.addTag(ObservabilityConstants.TAG_KEY_ERROR, ObservabilityConstants.TAG_TRUE_VALUE);
            ctx.addTag(AzureFilesObserverContext.TAG_ERROR_TYPE, errorType);
            ctx.addTag(AzureFilesObserverContext.TAG_FAILURE_REASON, errorType);
            ctx.addTag(AzureFilesObserverContext.TAG_OUTCOME, AzureFilesMetricsUtil.OUTCOME_FAILURE);
        } catch (Throwable t) {
            log.debug("Failed to send error metrics on current frame", t);
        }
    }

    /**
     * Adds file.size and file.modified_time as span-only tags to strand properties.
     *
     * @param strandProperties the strand properties map (may be null)
     * @param fileSize         file size in bytes, or -1 if unknown
     * @param modifiedTime     last-modified timestamp, or -1 if unknown
     * @param filePath         file path for span-only tag
     */
    public static void addFileMetadataToStrandProperties(Map<String, Object> strandProperties,
                                                          long fileSize, long modifiedTime, String filePath) {
        if (strandProperties == null) {
            return;
        }
        try {
            AzureFilesObserverContext ctx = (AzureFilesObserverContext) strandProperties.get(
                    ObservabilityConstants.KEY_OBSERVER_CONTEXT);
            if (ctx == null) {
                return;
            }
            if (fileSize >= 0) {
                ctx.addProperty(AzureFilesObserverContext.TAG_FILE_SIZE, fileSize);
            }
            if (modifiedTime >= 0) {
                ctx.addProperty(AzureFilesObserverContext.TAG_FILE_MODIFIED_TIME, modifiedTime);
            }
            if (filePath != null) {
                ctx.addProperty(AzureFilesObserverContext.TAG_FILE_PATH, filePath);
            }
        } catch (Throwable t) {
            log.debug("Failed to add file metadata to strand properties", t);
        }
    }

    /**
     * Tags the outcome of a client operation on the auto-instrumented span: success sets the
     * outcome tag, failure sets error tags. Returns the result unchanged.
     *
     * @param result the operation result
     * @param env    the current Ballerina environment
     * @return the result, unchanged
     */
    public static Object sendTraces(Object result, Environment env) {
        if (result instanceof BError bError) {
            String errorType = bError.getType() != null ? bError.getType().getName() : AzureFilesMetricsUtil.UNKNOWN;
            sendErrorMetricsOnCurrentFrame(env, errorType);
        } else {
            try {
                ObserverContext ctx = ObserveUtils.getObserverContextOfCurrentFrame(env);
                if (ctx != null) {
                    ctx.addTag(AzureFilesObserverContext.TAG_OUTCOME, AzureFilesMetricsUtil.OUTCOME_SUCCESS);
                }
            } catch (Throwable t) {
                log.debug("Failed to send success traces", t);
            }
        }
        return result;
    }
}
