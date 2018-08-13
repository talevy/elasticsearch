/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle.action;

import org.elasticsearch.protocol.xpack.indexlifecycle.GetIndexLifecyclePolicyRequest;
import org.elasticsearch.test.AbstractStreamableTestCase;

import java.util.Arrays;

public class GetLifecycleRequestTests extends AbstractStreamableTestCase<GetIndexLifecyclePolicyRequest> {

    @Override
    protected GetIndexLifecyclePolicyRequest createTestInstance() {
        return new GetIndexLifecyclePolicyRequest(randomAlphaOfLengthBetween(1, 20));
    }

    @Override
    protected GetIndexLifecyclePolicyRequest createBlankInstance() {
        return new GetIndexLifecyclePolicyRequest();
    }

    @Override
    protected GetIndexLifecyclePolicyRequest mutateInstance(GetIndexLifecyclePolicyRequest request) {
        String[] originalPolicies = request.getPolicyNames();
        String[] newPolicies = Arrays.copyOf(originalPolicies, originalPolicies.length + 1);
        newPolicies[originalPolicies.length] = randomAlphaOfLength(5);
        return new GetIndexLifecyclePolicyRequest(newPolicies);
    }
}
