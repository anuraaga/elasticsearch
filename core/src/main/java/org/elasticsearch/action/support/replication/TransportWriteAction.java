/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.support.replication;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.WriteResponse;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.Translog.Location;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Base class for transport actions that modify data in some shard like index, delete, and shardBulk.
 */
public abstract class TransportWriteAction<
            Request extends ReplicatedWriteRequest<Request>,
            ReplicaRequest extends ReplicatedWriteRequest<ReplicaRequest>,
            Response extends ReplicationResponse & WriteResponse
        > extends TransportReplicationAction<Request, ReplicaRequest, Response> {

    protected TransportWriteAction(Settings settings, String actionName, TransportService transportService,
            ClusterService clusterService, IndicesService indicesService, ThreadPool threadPool, ShardStateAction shardStateAction,
            ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver, Supplier<Request> request,
                                   Supplier<ReplicaRequest> replicaRequest, String executor) {
        super(settings, actionName, transportService, clusterService, indicesService, threadPool, shardStateAction, actionFilters,
                indexNameExpressionResolver, request, replicaRequest, executor);
    }

    /**
     * Called on the primary with a reference to the primary {@linkplain IndexShard} to modify.
     */
    protected abstract PrimaryOperationResult<Response> onPrimaryShard(Request request, IndexShard primary) throws Exception;

    /**
     * Called once per replica with a reference to the replica {@linkplain IndexShard} to modify.
     *
     * @return the result of the replication operation containing either the translog location of the {@linkplain IndexShard}
     * after the write was completed or a failure if the operation failed
     */
    protected abstract ReplicaOperationResult onReplicaShard(ReplicaRequest request, IndexShard replica) throws Exception;

    @Override
    protected final WritePrimaryResult shardOperationOnPrimary(Request request, IndexShard primary) throws Exception {
        final PrimaryOperationResult<Response> result = onPrimaryShard(request, primary);
        return result.success()
                ? new WritePrimaryResult((ReplicaRequest) request, result.getResponse(), result.getLocation(), primary)
                : new WritePrimaryResult(result.getFailure());
    }

    @Override
    protected final WriteReplicaResult shardOperationOnReplica(ReplicaRequest request, IndexShard replica) throws Exception {
        final ReplicaOperationResult result = onReplicaShard(request, replica);
        return result.success()
                ? new WriteReplicaResult(request, result.getLocation(), replica)
                : new WriteReplicaResult(result.getFailure());
    }

    abstract static class OperationWriteResult {
        private final Translog.Location location;
        private final Exception failure;

        protected OperationWriteResult(@Nullable Location location) {
            this.location = location;
            this.failure = null;
        }

        protected OperationWriteResult(Exception failure) {
            this.location = null;
            this.failure = failure;
        }

        public Translog.Location getLocation() {
            return location;
        }

        public Exception getFailure() {
            return failure;
        }

        public boolean success() {
            return failure == null;
        }
    }

    /**
     * Simple result from a primary write action (includes response).
     * Write actions have static method to return these so they can integrate with bulk.
     */
    public static class PrimaryOperationResult<Response extends ReplicationResponse> extends OperationWriteResult {
        private final Response response;

        public PrimaryOperationResult(Response response, @Nullable Location location) {
            super(location);
            this.response = response;
        }

        public PrimaryOperationResult(Exception failure) {
            super(failure);
            this.response = null;
        }

        public Response getResponse() {
            return response;
        }
    }

    /**
     * Simple result from a replica write action. Write actions have static method to return these so they can integrate with bulk.
     */
    public static class ReplicaOperationResult extends OperationWriteResult {

        public ReplicaOperationResult(@Nullable Location location) {
            super(location);
        }

        public ReplicaOperationResult(Exception failure) {
            super(failure);
        }
    }

    /**
     * Result of taking the action on the primary.
     */
    class WritePrimaryResult extends PrimaryResult implements RespondingWriteResult {
        boolean finishedAsyncActions;
        ActionListener<Response> listener = null;

        public WritePrimaryResult(ReplicaRequest request, Response finalResponse,
                                  @Nullable Location location, IndexShard primary) {
            super(request, finalResponse);
            /*
             * We call this before replication because this might wait for a refresh and that can take a while. This way we wait for the
             * refresh in parallel on the primary and on the replica.
             */
            new AsyncAfterWriteAction(primary, request, location, this, logger).run();
        }

        public WritePrimaryResult(Exception failure) {
            super(failure);
            this.finishedAsyncActions = true;
        }

        @Override
        public synchronized void respond(ActionListener<Response> listener) {
            this.listener = listener;
            respondIfPossible(null);
        }

        /**
         * Respond if the refresh has occurred and the listener is ready. Always called while synchronized on {@code this}.
         */
        protected void respondIfPossible(Exception ex) {
            if (finishedAsyncActions && listener != null) {
                if (ex == null) {
                    super.respond(listener);
                } else {
                    listener.onFailure(ex);
                }
            }
        }

        public synchronized void onFailure(Exception exception) {
            finishedAsyncActions = true;
            respondIfPossible(exception);
        }

        @Override
        public synchronized void onSuccess(boolean forcedRefresh) {
            if (finalResponseIfSuccessful != null) {
                finalResponseIfSuccessful.setForcedRefresh(forcedRefresh);
            }
            finishedAsyncActions = true;
            respondIfPossible(null);
        }
    }

    /**
     * Result of taking the action on the replica.
     */
    class WriteReplicaResult extends ReplicaResult implements RespondingWriteResult {
        boolean finishedAsyncActions;
        private ActionListener<TransportResponse.Empty> listener;

        public WriteReplicaResult(ReplicaRequest request, Location location, IndexShard replica) {
            new AsyncAfterWriteAction(replica, request, location, this, logger).run();
        }

        public WriteReplicaResult(Exception finalFailure) {
            super(finalFailure);
            this.finishedAsyncActions = true;
        }

        @Override
        public void respond(ActionListener<TransportResponse.Empty> listener) {
            this.listener = listener;
            respondIfPossible(null);
        }

        /**
         * Respond if the refresh has occurred and the listener is ready. Always called while synchronized on {@code this}.
         */
        protected void respondIfPossible(Exception ex) {
            if (finishedAsyncActions && listener != null) {
                if (ex == null) {
                    super.respond(listener);
                } else {
                    listener.onFailure(ex);
                }
            }
        }

        @Override
        public void onFailure(Exception ex) {
            finishedAsyncActions = true;
            respondIfPossible(ex);
        }

        @Override
        public synchronized void onSuccess(boolean forcedRefresh) {
            finishedAsyncActions = true;
            respondIfPossible(null);
        }
    }

    /**
     * callback used by {@link AsyncAfterWriteAction} to notify that all post
     * process actions have been executed
     */
    interface RespondingWriteResult {
        /**
         * Called on successful processing of all post write actions
         * @param forcedRefresh <code>true</code> iff this write has caused a refresh
         */
        void onSuccess(boolean forcedRefresh);

        /**
         * Called on failure if a post action failed.
         */
        void onFailure(Exception ex);
    }

    /**
     * This class encapsulates post write actions like async waits for
     * translog syncs or waiting for a refresh to happen making the write operation
     * visible.
     */
    static final class AsyncAfterWriteAction {
        private final Location location;
        private final boolean waitUntilRefresh;
        private final boolean sync;
        private final AtomicInteger pendingOps = new AtomicInteger(1);
        private final AtomicBoolean refreshed = new AtomicBoolean(false);
        private final AtomicReference<Exception> syncFailure = new AtomicReference<>(null);
        private final RespondingWriteResult respond;
        private final IndexShard indexShard;
        private final WriteRequest<?> request;
        private final Logger logger;

        AsyncAfterWriteAction(final IndexShard indexShard,
                             final WriteRequest<?> request,
                             @Nullable final Translog.Location location,
                             final RespondingWriteResult respond,
                             final Logger logger) {
            this.indexShard = indexShard;
            this.request = request;
            boolean waitUntilRefresh = false;
            switch (request.getRefreshPolicy()) {
                case IMMEDIATE:
                    indexShard.refresh("refresh_flag_index");
                    refreshed.set(true);
                    break;
                case WAIT_UNTIL:
                    if (location != null) {
                        waitUntilRefresh = true;
                        pendingOps.incrementAndGet();
                    }
                    break;
                case NONE:
                    break;
                default:
                    throw new IllegalArgumentException("unknown refresh policy: " + request.getRefreshPolicy());
            }
            this.waitUntilRefresh = waitUntilRefresh;
            this.respond = respond;
            this.location = location;
            if ((sync = indexShard.getTranslogDurability() == Translog.Durability.REQUEST && location != null)) {
                pendingOps.incrementAndGet();
            }
            this.logger = logger;
            assert pendingOps.get() >= 0 && pendingOps.get() <= 3 : "pendingOpts was: " + pendingOps.get();
        }

        /** calls the response listener if all pending operations have returned otherwise it just decrements the pending opts counter.*/
        private void maybeFinish() {
            final int numPending = pendingOps.decrementAndGet();
            if (numPending == 0) {
                if (syncFailure.get() != null) {
                    respond.onFailure(syncFailure.get());
                } else {
                    respond.onSuccess(refreshed.get());
                }
            }
            assert numPending >= 0 && numPending <= 2: "numPending must either 2, 1 or 0 but was " + numPending ;
        }

        void run() {
            // we either respond immediately ie. if we we don't fsync per request or wait for refresh
            // OR we got an pass async operations on and wait for them to return to respond.
            indexShard.maybeFlush();
            maybeFinish(); // decrement the pendingOpts by one, if there is nothing else to do we just respond with success.
            if (waitUntilRefresh) {
                assert pendingOps.get() > 0;
                indexShard.addRefreshListener(location, forcedRefresh -> {
                    if (forcedRefresh) {
                        logger.warn("block_until_refresh request ran out of slots and forced a refresh: [{}]", request);
                    }
                    refreshed.set(forcedRefresh);
                    maybeFinish();
                });
            }
            if (sync) {
                assert pendingOps.get() > 0;
                indexShard.sync(location, (ex) -> {
                    syncFailure.set(ex);
                    maybeFinish();
                });
            }
        }
    }
}
