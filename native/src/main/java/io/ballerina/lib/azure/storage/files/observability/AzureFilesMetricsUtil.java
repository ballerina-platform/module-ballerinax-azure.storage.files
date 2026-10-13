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

import io.ballerina.runtime.observability.ObserveUtils;
import io.ballerina.runtime.observability.metrics.DefaultMetricRegistry;
import io.ballerina.runtime.observability.metrics.MetricId;
import io.ballerina.runtime.observability.metrics.MetricRegistry;
import io.ballerina.runtime.observability.metrics.StatisticConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.time.Duration;

/**
 * Utility class for recording Azure Files connector metrics.
 *
 * <p>All public methods swallow exceptions internally so that observability failures
 * never break file operations.
 *
 * <p>Metrics published:
 * <ul>
 *   <li>{@code file_active_connections} (gauge) — number of active Azure Files connections</li>
 *   <li>{@code file_bytes_transferred_total} (counter) — bytes read or written</li>
 *   <li>{@code file_events_total} (counter) — file lifecycle and poll events</li>
 *   <li>{@code file_databinding_duration_seconds} (gauge with percentiles) — content binding time</li>
 *   <li>{@code file_resource_execution_duration_seconds} (gauge with percentiles) — handler execution time</li>
 * </ul>
 */
public final class AzureFilesMetricsUtil {

    private static final Logger log = LoggerFactory.getLogger(AzureFilesMetricsUtil.class);
    private static final String FILE_CONNECTOR_NAME = "file";
    private static final String[] METRIC_ACTIVE_CONNECTIONS = {
            "active_connections", "Number of active Azure Files connections"};
    private static final String[] METRIC_BYTES_TRANSFERRED = {
            "bytes_transferred_total", "Total bytes read or written across operations"};
    private static final String[] METRIC_FILE_EVENTS = {
            "events_total", "Total file lifecycle and poll events"};
    private static final String[] METRIC_DATABINDING_DURATION = {
            "databinding_duration_seconds", "Time taken to fetch and convert file content"};
    private static final String[] METRIC_RESOURCE_EXECUTION_DURATION = {
            "resource_execution_duration_seconds", "Time taken to execute the resource/handler method"};

    private static final StatisticConfig DURATION_STATISTIC_CONFIG = StatisticConfig.builder()
            .percentiles(0.5, 0.75, 0.9, 0.95, 0.99)
            .expiry(Duration.ofMinutes(5))
            .buckets(10)
            .build();

    /** Sentinel used when a URL or protocol value is unavailable. */
    public static final String UNKNOWN = "unknown";

    /** Sentinel used when a tag is not applicable for a given stage, ensuring consistent label sets. */
    public static final String NONE = "none";

    /** Module tag value identifying the Azure Files module. */
    public static final String MODULE_AZURE_FILES = "azure_files";

    /** Context tag value for client operations. */
    public static final String CONTEXT_CLIENT = "client";

    /** Context tag value for listener operations. */
    public static final String CONTEXT_LISTENER = "listener";

    /** Event-type tag value: a file was added or created. */
    public static final String EVENT_TYPE_CHANGE = "create";

    /** Event-type tag value: content-binding error dispatched to {@code onError}. */
    public static final String EVENT_TYPE_ERROR = "error";

    /** Operation-type tag value: read file content. */
    public static final String OPERATION_TYPE_GET = "get";

    /** Operation-type tag value: write file content. */
    public static final String OPERATION_TYPE_PUT = "put";

    /** Operation-type tag value: admin/filesystem operations (delete, rename, move, copy, mkdir, etc.). */
    public static final String OPERATION_TYPE_MANAGE = "manage";

    /** Action-type tag value for client operations (get, put, delete, etc.). */
    public static final String ACTION_TYPE_OPERATION = "client_operation";

    /** Action-type tag value for listener event dispatches (create, delete, error). */
    public static final String ACTION_TYPE_EVENT = "file_event";

    /** Action-type tag value for poll cycles. */
    public static final String ACTION_TYPE_POLL = "poll_cycle";

    // File lifecycle stage constants
    /** File stage: file discovered during poll or event. */
    public static final String FILE_STAGE_FOUND = "found";
    /** File stage: file matched to a handler and handed over. */
    public static final String FILE_STAGE_DISPATCHED = "dispatched";
    /** File stage: handler invocation completed. */
    public static final String FILE_STAGE_HANDLED = "handled";
    /** File stage: post-processing action completed (move/delete). */
    public static final String FILE_STAGE_CLEANED_UP = "cleaned_up";

    // Outcome constants
    /** Outcome: operation succeeded. */
    public static final String OUTCOME_SUCCESS = "success";
    /** Outcome: operation failed. */
    public static final String OUTCOME_FAILURE = "failure";
    /** Outcome: file found but skipped (e.g. no handler matched). */
    public static final String OUTCOME_SKIPPED = "skipped";

    // Cleanup action constants
    /** Cleanup action: file moved after processing. */
    public static final String CLEANUP_ACTION_MOVE = "move";
    /** Cleanup action: file deleted after processing. */
    public static final String CLEANUP_ACTION_DELETE = "delete";

    // Failure reason constants
    /** Failure reason: file found but no handler matched. */
    public static final String FAILURE_NO_HANDLER_MATCHED = "no_handler_matched";
    /** Failure reason: content binding failed. */
    public static final String FAILURE_BINDING_FAILED = "binding_failed";
    /** Failure reason: move post-processing failed. */
    public static final String FAILURE_MOVE_FAILED = "move_failed";
    /** Failure reason: delete post-processing failed. */
    public static final String FAILURE_DELETE_FAILED = "delete_failed";

    private static final MetricRegistry metricRegistry = DefaultMetricRegistry.getInstance();

    private static String instanceUrl;
    private static boolean instanceUrlResolved;

    /** Returns the hostname of the current instance, resolved lazily on first use, or {@code null} if unavailable. */
    public static String getInstanceUrl() {
        if (!instanceUrlResolved) {
            try {
                instanceUrl = InetAddress.getLocalHost().getHostName();
            } catch (Exception e) {
                instanceUrl = null;
            }
            instanceUrlResolved = true;
        }
        return instanceUrl;
    }

    /**
     * Increments the {@code file_active_connections} gauge when a new client or listener is created.
     *
     * @param url      remote URL
     * @param protocol wire protocol (e.g. "https")
     * @param context  {@link #CONTEXT_CLIENT} or {@link #CONTEXT_LISTENER}
     */
    public static void reportNewConnection(String url, String protocol, String context) {
        if (!ObserveUtils.isMetricsEnabled()) {
            return;
        }
        try {
            AzureFilesObserverContext observerContext = new AzureFilesObserverContext(context, url, protocol);
            metricRegistry.gauge(new MetricId(FILE_CONNECTOR_NAME + "_" + METRIC_ACTIVE_CONNECTIONS[0],
                    METRIC_ACTIVE_CONNECTIONS[1], observerContext.getAllTags())).increment();
        } catch (Throwable t) {
            log.debug("Failed to report new connection metric", t);
        }
    }

    /**
     * Decrements the {@code file_active_connections} gauge when a client or listener is closed.
     *
     * @param url      remote URL
     * @param protocol wire protocol
     * @param context  client or listener
     */
    public static void reportConnectionClose(String url, String protocol, String context) {
        if (!ObserveUtils.isMetricsEnabled()) {
            return;
        }
        try {
            AzureFilesObserverContext observerContext = new AzureFilesObserverContext(context, url, protocol);
            metricRegistry.gauge(new MetricId(FILE_CONNECTOR_NAME + "_" + METRIC_ACTIVE_CONNECTIONS[0],
                    METRIC_ACTIVE_CONNECTIONS[1], observerContext.getAllTags())).decrement();
        } catch (Throwable t) {
            log.debug("Failed to report connection close metric", t);
        }
    }

    /**
     * Reports bytes transferred during a file operation (get or put).
     *
     * @param url           remote URL
     * @param protocol      wire protocol
     * @param context       {@link #CONTEXT_CLIENT} or {@link #CONTEXT_LISTENER}
     * @param operationType {@link #OPERATION_TYPE_GET} or {@link #OPERATION_TYPE_PUT}
     * @param bytes         number of bytes transferred
     */
    public static void reportBytesTransferred(String url, String protocol, String context,
                                              String operationType, long bytes) {
        if (!ObserveUtils.isMetricsEnabled() || bytes <= 0) {
            return;
        }
        try {
            AzureFilesObserverContext observerContext = new AzureFilesObserverContext(context, url, protocol);
            observerContext.addTag(AzureFilesObserverContext.TAG_OPERATION_TYPE, operationType);
            metricRegistry.counter(new MetricId(FILE_CONNECTOR_NAME + "_" + METRIC_BYTES_TRANSFERRED[0],
                    METRIC_BYTES_TRANSFERRED[1], observerContext.getAllTags())).increment(bytes);
        } catch (Throwable t) {
            log.debug("Failed to report bytes transferred metric", t);
        }
    }

    /**
     * Reports a file lifecycle stage event.
     *
     * @param url           remote URL
     * @param protocol      wire protocol
     * @param watchedPath   monitored directory path, or {@code null}
     * @param fileStage     lifecycle stage (found, dispatched, handled, cleaned_up)
     * @param outcome       outcome tag value, or {@code null}
     * @param errorType     error type, or {@code null}
     * @param handlerName   handler method name, or {@code null}
     */
    public static void reportFileStage(String url, String protocol, String watchedPath, String fileStage,
                                       String outcome, String errorType, String handlerName) {
        reportFileStage(url, protocol, watchedPath, fileStage, outcome, errorType, handlerName, NONE);
    }

    public static void reportFileStage(String url, String protocol, String watchedPath, String fileStage,
                                       String outcome, String errorType, String handlerName, String cleanupAction) {
        if (!ObserveUtils.isMetricsEnabled()) {
            return;
        }
        try {
            AzureFilesObserverContext observerContext = new AzureFilesObserverContext(CONTEXT_LISTENER, url, protocol);
            observerContext.addTag(AzureFilesObserverContext.TAG_ACTION_TYPE, ACTION_TYPE_EVENT);
            observerContext.addTag(AzureFilesObserverContext.TAG_FILE_STAGE, fileStage);
            observerContext.addTag(AzureFilesObserverContext.TAG_EVENT_TYPE,
                    fileStage.equals(FILE_STAGE_CLEANED_UP) ? NONE : EVENT_TYPE_CHANGE);
            observerContext.addTag(AzureFilesObserverContext.TAG_OPERATION_TYPE, NONE);
            observerContext.addTag(AzureFilesObserverContext.TAG_WATCHED_PATH,
                    watchedPath != null ? watchedPath : NONE);
            observerContext.addTag(AzureFilesObserverContext.TAG_OUTCOME,
                    outcome != null ? outcome : NONE);
            observerContext.addTag(AzureFilesObserverContext.TAG_ERROR_TYPE,
                    errorType != null ? errorType : NONE);
            observerContext.addTag(AzureFilesObserverContext.TAG_FAILURE_REASON,
                    errorType != null ? errorType : NONE);
            observerContext.addTag(AzureFilesObserverContext.TAG_HANDLER_NAME,
                    handlerName != null ? handlerName : NONE);
            observerContext.addTag(AzureFilesObserverContext.TAG_CLEANUP_ACTION,
                    cleanupAction != null ? cleanupAction : NONE);
            String instanceUrl = getInstanceUrl();
            observerContext.addTag(AzureFilesObserverContext.TAG_INSTANCE_URL,
                    instanceUrl != null ? instanceUrl : NONE);
            metricRegistry.counter(new MetricId(FILE_CONNECTOR_NAME + "_" + METRIC_FILE_EVENTS[0],
                    METRIC_FILE_EVENTS[1], observerContext.getAllTags())).increment();
        } catch (Throwable t) {
            log.debug("Failed to report file stage metric", t);
        }
    }

    /**
     * Reports a poll cycle completion.
     *
     * @param url         remote URL
     * @param protocol    wire protocol
     * @param watchedPath monitored directory path, or {@code null}
     * @param outcome     {@link #OUTCOME_SUCCESS} or {@link #OUTCOME_FAILURE}
     */
    public static void reportPollCycle(String url, String protocol, String watchedPath, String outcome) {
        if (!ObserveUtils.isMetricsEnabled()) {
            return;
        }
        try {
            AzureFilesObserverContext observerContext = new AzureFilesObserverContext(CONTEXT_LISTENER, url, protocol);
            observerContext.addTag(AzureFilesObserverContext.TAG_ACTION_TYPE, ACTION_TYPE_POLL);
            observerContext.addTag(AzureFilesObserverContext.TAG_FILE_STAGE, NONE);
            observerContext.addTag(AzureFilesObserverContext.TAG_EVENT_TYPE, NONE);
            observerContext.addTag(AzureFilesObserverContext.TAG_OPERATION_TYPE, NONE);
            observerContext.addTag(AzureFilesObserverContext.TAG_OUTCOME, outcome != null ? outcome : NONE);
            observerContext.addTag(AzureFilesObserverContext.TAG_ERROR_TYPE, NONE);
            observerContext.addTag(AzureFilesObserverContext.TAG_FAILURE_REASON, NONE);
            observerContext.addTag(AzureFilesObserverContext.TAG_HANDLER_NAME, NONE);
            observerContext.addTag(AzureFilesObserverContext.TAG_CLEANUP_ACTION, NONE);
            observerContext.addTag(AzureFilesObserverContext.TAG_WATCHED_PATH,
                    watchedPath != null ? watchedPath : NONE);
            String instanceUrl = getInstanceUrl();
            observerContext.addTag(AzureFilesObserverContext.TAG_INSTANCE_URL,
                    instanceUrl != null ? instanceUrl : NONE);
            metricRegistry.counter(new MetricId(FILE_CONNECTOR_NAME + "_" + METRIC_FILE_EVENTS[0],
                    METRIC_FILE_EVENTS[1], observerContext.getAllTags())).increment();
        } catch (Throwable t) {
            log.debug("Failed to report poll cycle metric", t);
        }
    }

    /**
     * Reports the time taken to fetch and convert file content (data binding).
     *
     * @param url          remote URL
     * @param protocol     wire protocol
     * @param handlerName  handler method name (e.g. "onFileJson")
     * @param outcome      {@link #OUTCOME_SUCCESS} or {@link #OUTCOME_FAILURE}
     * @param durationMs   duration in milliseconds (converted to seconds before recording)
     */
    public static void reportDatabindingDuration(String url, String protocol, String handlerName,
                                                  String outcome, long durationMs) {
        if (!ObserveUtils.isMetricsEnabled()) {
            return;
        }
        try {
            AzureFilesObserverContext observerContext = new AzureFilesObserverContext(CONTEXT_LISTENER, url, protocol);
            observerContext.addTag(AzureFilesObserverContext.TAG_HANDLER_NAME,
                    handlerName != null ? handlerName : NONE);
            observerContext.addTag(AzureFilesObserverContext.TAG_OUTCOME,
                    outcome != null ? outcome : NONE);
            metricRegistry.gauge(new MetricId(FILE_CONNECTOR_NAME + "_" + METRIC_DATABINDING_DURATION[0],
                    METRIC_DATABINDING_DURATION[1], observerContext.getAllTags()),
                    DURATION_STATISTIC_CONFIG).setValue(durationMs / 1000.0);
        } catch (Throwable t) {
            log.debug("Failed to report databinding duration metric", t);
        }
    }

    /**
     * Reports the time taken to execute the user's resource/handler method.
     *
     * @param url          remote URL
     * @param protocol     wire protocol
     * @param handlerName  handler method name (e.g. "onFileJson")
     * @param outcome      {@link #OUTCOME_SUCCESS} or {@link #OUTCOME_FAILURE}
     * @param durationMs   duration in milliseconds (converted to seconds before recording)
     */
    public static void reportResourceExecutionDuration(String url, String protocol, String handlerName,
                                                        String outcome, long durationMs) {
        if (!ObserveUtils.isMetricsEnabled()) {
            return;
        }
        try {
            AzureFilesObserverContext observerContext = new AzureFilesObserverContext(CONTEXT_LISTENER, url, protocol);
            observerContext.addTag(AzureFilesObserverContext.TAG_HANDLER_NAME,
                    handlerName != null ? handlerName : NONE);
            observerContext.addTag(AzureFilesObserverContext.TAG_OUTCOME,
                    outcome != null ? outcome : NONE);
            metricRegistry.gauge(new MetricId(FILE_CONNECTOR_NAME + "_" + METRIC_RESOURCE_EXECUTION_DURATION[0],
                    METRIC_RESOURCE_EXECUTION_DURATION[1], observerContext.getAllTags()),
                    DURATION_STATISTIC_CONFIG).setValue(durationMs / 1000.0);
        } catch (Throwable t) {
            log.debug("Failed to report resource execution duration metric", t);
        }
    }

    private AzureFilesMetricsUtil() {
    }
}
