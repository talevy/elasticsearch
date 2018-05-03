/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 *
 */
package org.elasticsearch.xpack.indexlifecycle.action;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xpack.core.indexlifecycle.Step.StepKey;
import org.elasticsearch.xpack.core.indexlifecycle.action.MoveToStepLifecycleAction;
import org.elasticsearch.xpack.indexlifecycle.IndexLifecycle;

import java.io.IOException;

public class RestPostMoveToStepAction extends BaseRestHandler {

    public RestPostMoveToStepAction(Settings settings, RestController controller) {
        super(settings);
        controller.registerHandler(RestRequest.Method.POST, IndexLifecycle.BASE_PATH + "_move/{name}", this);
    }

    @Override
    public String getName() {
        return "xpack_lifecycle_move_to_step_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        String index = restRequest.param("name");
        XContentParser parser = restRequest.contentParser();
        MoveToStepLifecycleAction.Request moveToStep = new MoveToStepLifecycleAction.Request(index,
            new StepKey(phase, action, stepName));
        moveToStep.timeout(restRequest.paramAsTime("timeout", moveToStep.timeout()));
        moveToStep.masterNodeTimeout(restRequest.paramAsTime("master_timeout", moveToStep.masterNodeTimeout()));

        return channel -> client.execute(MoveToStepLifecycleAction.INSTANCE, moveToStep, new RestToXContentListener<>(channel));
    }
}
