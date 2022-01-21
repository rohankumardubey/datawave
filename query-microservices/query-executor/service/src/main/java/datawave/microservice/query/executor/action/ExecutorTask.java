package datawave.microservice.query.executor.action;

import datawave.microservice.query.config.QueryProperties;
import datawave.microservice.query.executor.QueryExecutor;
import datawave.microservice.query.executor.config.ExecutorProperties;
import datawave.microservice.query.messaging.QueryResultsManager;
import datawave.microservice.query.messaging.QueryResultsPublisher;
import datawave.microservice.query.messaging.Result;
import datawave.microservice.query.remote.QueryRequest;
import datawave.microservice.query.storage.CachedQueryStatus;
import datawave.microservice.query.storage.QueryStatus;
import datawave.microservice.query.storage.QueryStorageCache;
import datawave.microservice.query.storage.QueryTask;
import datawave.microservice.query.storage.TaskKey;
import datawave.microservice.query.storage.TaskStates;
import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.QueryMetricClient;
import datawave.microservice.querymetric.QueryMetricFactory;
import datawave.microservice.querymetric.QueryMetricType;
import datawave.services.common.connection.AccumuloConnectionFactory;
import datawave.services.query.logic.CheckpointableQueryLogic;
import datawave.services.query.logic.QueryCheckpoint;
import datawave.services.query.logic.QueryKey;
import datawave.services.query.logic.QueryLogic;
import datawave.services.query.logic.QueryLogicFactory;
import datawave.services.query.logic.WritesQueryMetrics;
import datawave.services.query.runner.AccumuloConnectionRequestMap;
import datawave.webservice.query.Query;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;
import org.apache.accumulo.core.client.Connector;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.apache.log4j.Logger;
import org.springframework.cloud.bus.BusProperties;
import org.springframework.cloud.bus.event.RemoteQueryRequestEvent;
import org.springframework.context.ApplicationEventPublisher;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.UUID;

public abstract class ExecutorTask implements Runnable {
    
    private static final Logger log = Logger.getLogger(ExecutorTask.class);
    
    protected final QueryExecutor source;
    protected final AccumuloConnectionRequestMap connectionMap;
    protected final AccumuloConnectionFactory connectionFactory;
    protected final QueryStorageCache cache;
    protected final QueryResultsManager resultsManager;
    protected final QueryLogicFactory queryLogicFactory;
    protected final BusProperties busProperties;
    protected final QueryProperties queryProperties;
    protected final ExecutorProperties executorProperties;
    protected final ApplicationEventPublisher publisher;
    protected final QueryMetricFactory metricFactory;
    protected final QueryMetricClient metricClient;
    protected final QueryTask task;
    protected boolean interrupted = false;
    
    public ExecutorTask(QueryExecutor source, QueryTask task) {
        this(source, source.getExecutorProperties(), source.getQueryProperties(), source.getBusProperties(), source.getConnectionRequestMap(),
                        source.getConnectionFactory(), source.getCache(), source.getQueues(), source.getQueryLogicFactory(), source.getPublisher(),
                        source.getMetricFactory(), source.getMetricClient(), task);
    }
    
    public ExecutorTask(QueryExecutor source, ExecutorProperties executorProperties, QueryProperties queryProperties, BusProperties busProperties,
                    AccumuloConnectionRequestMap connectionMap, AccumuloConnectionFactory connectionFactory, QueryStorageCache cache,
                    QueryResultsManager resultsManager, QueryLogicFactory queryLogicFactory, ApplicationEventPublisher publisher,
                    QueryMetricFactory metricFactory, QueryMetricClient metricClient, QueryTask task) {
        this.source = source;
        this.executorProperties = executorProperties;
        this.queryProperties = queryProperties;
        this.busProperties = busProperties;
        this.cache = cache;
        this.resultsManager = resultsManager;
        this.connectionMap = connectionMap;
        this.connectionFactory = connectionFactory;
        this.queryLogicFactory = queryLogicFactory;
        this.publisher = publisher;
        this.metricFactory = metricFactory;
        this.metricClient = metricClient;
        this.task = task;
    }
    
    public TaskKey getTaskKey() {
        return task.getTaskKey();
    }
    
    /**
     * Execute the task
     * 
     * @return True if the task was completed, false otherwise.
     * @throws Exception
     *             is the task failed
     */
    public abstract boolean executeTask(CachedQueryStatus status, Connector connector) throws Exception;
    
    /**
     * Interrupt this execution
     */
    public void interrupt() {
        interrupted = true;
    }
    
    /**
     * It is presumed that a lock for this task has already been obtained by the QueryExecutor
     */
    @Override
    public void run() {
        
        boolean taskComplete = false;
        boolean taskFailed = false;
        
        TaskKey taskKey = task.getTaskKey();
        log.debug("Running " + taskKey);
        
        String queryId = taskKey.getQueryId();
        
        Connector connector = null;
        
        try {
            CachedQueryStatus queryStatus = new CachedQueryStatus(cache, queryId, executorProperties.getQueryStatusExpirationMs());
            log.debug("Getting connector for " + taskKey);
            connector = getConnector(queryStatus, AccumuloConnectionFactory.Priority.LOW);
            log.debug("Executing task for " + taskKey);
            taskComplete = executeTask(queryStatus, connector);
        } catch (Exception e) {
            log.error("Failed to process task " + taskKey, e);
            taskFailed = true;
            DatawaveErrorCode errorCode = DatawaveErrorCode.QUERY_EXECUTION_ERROR;
            cache.updateFailedQueryStatus(taskKey.getQueryId(), e);
        } finally {
            if (connector != null) {
                try {
                    connectionFactory.returnConnection(connector);
                } catch (Exception e) {
                    log.error("Failed to return connection for " + taskKey);
                }
            }
            
            if (taskComplete) {
                cache.updateTaskState(taskKey, TaskStates.TASK_STATE.COMPLETED);
                try {
                    cache.deleteTask(taskKey);
                } catch (IOException e) {
                    log.error("We may be leaving an orphaned task: " + taskKey, e);
                }
            } else if (taskFailed) {
                cache.updateTaskState(taskKey, TaskStates.TASK_STATE.FAILED);
            } else {
                cache.updateTaskState(taskKey, TaskStates.TASK_STATE.READY);
                // more work to do on this task, lets notify
                publishExecutorEvent(QueryRequest.next(queryId), task.getTaskKey().getQueryPool());
            }
        }
    }
    
    /**
     * Checkpoint a query logic
     *
     * @param queryKey
     *            The query key
     * @param cpQueryLogic
     *            The checkpointable query logic
     * @throws IOException
     *             if checkpointing fails
     */
    protected void checkpoint(QueryKey queryKey, CheckpointableQueryLogic cpQueryLogic) throws IOException {
        for (QueryCheckpoint cp : cpQueryLogic.checkpoint(queryKey)) {
            log.debug("Storing a query checkpoint: " + cp);
            cache.createTask(QueryRequest.Method.NEXT, cp);
            publishExecutorEvent(QueryRequest.next(queryKey.getQueryId()), queryKey.getQueryPool());
        }
    }
    
    protected Connector getConnector(QueryStatus status, AccumuloConnectionFactory.Priority priority) throws Exception {
        Map<String,String> trackingMap = connectionFactory.getTrackingMap(Thread.currentThread().getStackTrace());
        Query q = status.getQuery();
        if (q.getOwner() != null) {
            trackingMap.put(AccumuloConnectionFactory.QUERY_USER, q.getOwner());
        }
        if (q.getId() != null) {
            trackingMap.put(AccumuloConnectionFactory.QUERY_ID, q.getId().toString());
        }
        if (q.getQuery() != null) {
            trackingMap.put(AccumuloConnectionFactory.QUERY, q.getQuery());
        }
        connectionMap.requestBegin(q.getId().toString(), q.getUserDN(), trackingMap);
        try {
            return connectionFactory.getConnection(q.getUserDN(), q.getDnList(), status.getQueryKey().getQueryPool(), priority, trackingMap);
        } finally {
            connectionMap.requestEnd(q.getId().toString());
        }
    }
    
    protected QueryLogic<?> getQueryLogic(Query query) throws QueryException, CloneNotSupportedException {
        log.debug("Getting query logic for " + query.getQueryLogicName());
        return queryLogicFactory.getQueryLogic(query.getQueryLogicName());
    }
    
    protected boolean shouldGenerateMoreResults(boolean exhaust, TaskKey taskKey, int maxPageSize, long maxResults, QueryStatus queryStatus) {
        QueryStatus.QUERY_STATE state = queryStatus.getQueryState();
        int concurrentNextCalls = queryStatus.getActiveNextCalls();
        float bufferMultiplier = executorProperties.getAvailableResultsPageMultiplier();
        long numResultsGenerated = queryStatus.getNumResultsGenerated();
        
        // if the state is closed AND we don't have any ongoing next calls, then stop
        if (state == QueryStatus.QUERY_STATE.CLOSED) {
            if (concurrentNextCalls == 0) {
                log.debug("Not getting results for closed query " + taskKey);
                return false;
            } else {
                // we know these are the last next calls, so cap the buffer multiplier to 1
                bufferMultiplier = 1.0f;
            }
        }
        
        // if the state is canceled or failed, then stop
        if (state == QueryStatus.QUERY_STATE.CANCELED || state == QueryStatus.QUERY_STATE.FAILED) {
            log.debug("Not getting results for canceled or failed query " + taskKey);
            return false;
        }
        
        // if we have reached the max results for this query, then stop
        if (maxResults > 0 && queryStatus.getNumResultsGenerated() >= maxResults) {
            log.debug("max resuilts reached for " + taskKey);
            return false;
        }
        
        // if we are to exhaust the iterator, then continue generating results
        if (exhaust) {
            return true;
        }
        
        // get the queue size
        long queueSize = resultsManager.getNumResultsRemaining(taskKey.getQueryId());
        
        // calculate a result buffer size (pagesize * multiplier) adjusting for concurrent next calls
        long bufferSize = (long) (maxPageSize * Math.max(1, concurrentNextCalls) * bufferMultiplier);
        
        // cap the buffer size by max results
        if (maxResults > 0) {
            bufferSize = Math.min(bufferSize, maxResults - numResultsGenerated);
        }
        
        // we should return results if we have less than what we want to have buffered
        log.debug("Getting results if " + queueSize + " < " + bufferSize);
        return (queueSize < bufferSize);
    }
    
    protected boolean pullResults(TaskKey taskKey, QueryLogic queryLogic, CachedQueryStatus queryStatus, boolean exhaustIterator) throws Exception {
        // start the timer on the query status to ensure we flush numResultsGenerated updates periodically
        queryStatus.startTimer();
        try {
            String queryId = taskKey.getQueryId();
            TransformIterator iter = queryLogic.getTransformIterator(queryStatus.getQuery());
            long maxResults = queryLogic.getResultLimit(queryStatus.getQuery().getDnList());
            if (maxResults != queryLogic.getMaxResults()) {
                log.info("Maximum results set to " + maxResults + " instead of default " + queryLogic.getMaxResults() + ", user "
                                + queryStatus.getQuery().getUserDN() + " has a DN configured with a different limit");
            }
            if (queryStatus.getQuery().isMaxResultsOverridden()) {
                maxResults = Math.max(maxResults, queryStatus.getQuery().getMaxResultsOverride());
            }
            int pageSize = queryStatus.getQuery().getPagesize();
            if (queryLogic.getMaxPageSize() != 0) {
                pageSize = Math.min(pageSize, queryLogic.getMaxPageSize());
            }
            QueryResultsPublisher publisher = resultsManager.createPublisher(queryId);
            boolean running = shouldGenerateMoreResults(exhaustIterator, taskKey, pageSize, maxResults, queryStatus);
            int count = 0;
            while (running && iter.hasNext()) {
                count++;
                Object result = iter.next();
                log.trace("Generated result for " + taskKey + ": " + result);
                publisher.publish(new Result(UUID.randomUUID().toString(), result));
                queryStatus.incrementNumResultsGenerated(1);
                updateMetrics(queryId, queryStatus, iter);
                running = shouldGenerateMoreResults(exhaustIterator, taskKey, pageSize, maxResults, queryStatus);
            }
            log.debug("Generated " + count + " results for " + taskKey);
            updateMetrics(queryId, queryStatus, iter);
            
            return !iter.hasNext();
        } finally {
            queryStatus.stopTimer();
            queryStatus.forceCacheUpdateIfDirty();
        }
    }
    
    protected void updateMetrics(String queryId, CachedQueryStatus queryStatus, TransformIterator iter) {
        // regardless whether the transform iterator returned a result, it may have updated the metrics (next/seek calls etc.)
        if (iter.getTransformer() instanceof WritesQueryMetrics) {
            WritesQueryMetrics metrics = ((WritesQueryMetrics) iter.getTransformer());
            if (metrics.hasMetrics()) {
                BaseQueryMetric baseQueryMetric = metricFactory.createMetric();
                baseQueryMetric.setQueryId(queryId);
                baseQueryMetric.setSourceCount(metrics.getSourceCount());
                queryStatus.incrementNextCount(metrics.getNextCount());
                baseQueryMetric.setNextCount(metrics.getNextCount());
                queryStatus.incrementSeekCount(metrics.getSeekCount());
                baseQueryMetric.setSeekCount(metrics.getSeekCount());
                baseQueryMetric.setYieldCount(metrics.getYieldCount());
                baseQueryMetric.setDocRanges(metrics.getDocRanges());
                baseQueryMetric.setFiRanges(metrics.getFiRanges());
                baseQueryMetric.setLastUpdated(new Date(queryStatus.getLastUpdatedMillis()));
                try {
                    // @formatter:off
                    metricClient.submit(
                            new QueryMetricClient.Request.Builder()
                                    .withMetric(baseQueryMetric)
                                    .withMetricType(QueryMetricType.DISTRIBUTED)
                                    .build());
                    // @formatter:on
                    metrics.resetMetrics();
                } catch (Exception e) {
                    log.error("Error updating query metric", e);
                }
            }
        }
    }
    
    protected void publishExecutorEvent(QueryRequest queryRequest, String queryPool) {
        // @formatter:off
        publisher.publishEvent(
                new RemoteQueryRequestEvent(
                        source,
                        busProperties.getId(),
                        getPooledExecutorName(queryPool),
                        queryRequest));
        // @formatter:on
    }
    
    protected String getPooledExecutorName(String poolName) {
        return String.join("-", Arrays.asList(queryProperties.getExecutorServiceName(), poolName));
    }
    
}