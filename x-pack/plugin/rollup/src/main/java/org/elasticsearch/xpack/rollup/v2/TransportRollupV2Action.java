/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.rollup.v2;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.rollup.action.RollupV2Action;
import org.elasticsearch.xpack.core.rollup.job.RollupV2Job;

import java.io.IOException;

public class TransportRollupV2Action extends TransportMasterNodeAction<RollupV2Action.Request, RollupV2Action.Response> {

    private final PersistentTasksService persistentTasksService;
    private static final Logger logger = LogManager.getLogger(TransportRollupV2Action.class);

    @Inject
    public TransportRollupV2Action(
            final ThreadPool threadPool,
            final TransportService transportService,
            final ClusterService clusterService,
            final ActionFilters actionFilters,
            final IndexNameExpressionResolver indexNameExpressionResolver,
            final PersistentTasksService persistentTasksService) {
        super(
            RollupV2Action.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            RollupV2Action.Request::new,
            indexNameExpressionResolver);
        this.persistentTasksService = persistentTasksService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected RollupV2Action.Response read(StreamInput in) throws IOException {
        return new RollupV2Action.Response(in);
    }

    @Override
    protected void masterOperation(Task task, RollupV2Action.Request request, ClusterState state,
                                   ActionListener<RollupV2Action.Response> listener) throws Exception {

        ActionListener<PersistentTasksCustomMetadata.PersistentTask<RollupV2Job>> waitToStart =
            new ActionListener<>() {
                @Override
                public void onResponse(PersistentTasksCustomMetadata.PersistentTask<RollupV2Job> task) {
                    listener.onResponse(new RollupV2Action.Response(true));
                }

                @Override
                public void onFailure(Exception e) {
                    if (ExceptionsHelper.unwrapCause(e) instanceof ResourceAlreadyExistsException) {
                        e = new ElasticsearchStatusException("Cannot start rollup job [" + request.getIndex() +
                            "] because it has already been started", RestStatus.CONFLICT, e);
                    }
                    listener.onFailure(e);
                }
            };
        persistentTasksService.sendStartRequest("_rollup-" + request.getIndex(),
            "xpack/rollupV2/job", request.getRollupJob(), waitToStart);
    }

    @Override
    protected ClusterBlockException checkBlock(RollupV2Action.Request request, ClusterState state) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA_WRITE, request.getIndex());
    }
}
