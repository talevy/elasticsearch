/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.indexlifecycle.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.indexlifecycle.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.indexlifecycle.LifecyclePolicy;
import org.elasticsearch.xpack.core.indexlifecycle.action.GetLifecycleAction;
import org.elasticsearch.protocol.xpack.indexlifecycle.GetIndexLifecyclePolicyRequest;
import org.elasticsearch.xpack.core.indexlifecycle.action.GetIndexLifecyclePolicyResponse;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TransportGetLifecycleAction extends TransportMasterNodeAction<GetIndexLifecyclePolicyRequest, GetIndexLifecyclePolicyResponse> {

    @Inject
    public TransportGetLifecycleAction(Settings settings, TransportService transportService, ClusterService clusterService,
            ThreadPool threadPool, ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, GetLifecycleAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver,
                GetIndexLifecyclePolicyRequest::new);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected GetIndexLifecyclePolicyResponse newResponse() {
        return new GetIndexLifecyclePolicyResponse();
    }

    @Override
    protected void masterOperation(GetIndexLifecyclePolicyRequest request, ClusterState state, ActionListener<GetIndexLifecyclePolicyResponse> listener) throws Exception {
        IndexLifecycleMetadata metadata = clusterService.state().metaData().custom(IndexLifecycleMetadata.TYPE);
        if (metadata == null) {
            listener.onFailure(new ResourceNotFoundException("Lifecycle policy not found: {}", Arrays.toString(request.getPolicyNames())));
        } else {
            List<LifecyclePolicy> requestedPolicies;
            // if no policies explicitly provided, behave as if `*` was specified
            if (request.getPolicyNames().length == 0) {
                requestedPolicies = new ArrayList<>(metadata.getPolicies().values());
            } else {
                requestedPolicies = new ArrayList<>(request.getPolicyNames().length);
                for (String name : request.getPolicyNames()) {
                    LifecyclePolicy policy = metadata.getPolicies().get(name);
                    if (policy == null) {
                        listener.onFailure(new ResourceNotFoundException("Lifecycle policy not found: {}", name));
                        return;
                    }
                    requestedPolicies.add(policy);
                }
            }
            listener.onResponse(new GetIndexLifecyclePolicyResponse(requestedPolicies));
        }
    }

    @Override
    protected ClusterBlockException checkBlock(GetIndexLifecyclePolicyRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
