/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.xpack.core.indexlifecycle.Step.StepKey;

public abstract class AbstractStepTestCase<T extends Step> extends ESTestCase {

    protected static final int NUMBER_OF_TEST_RUNS = 20;

    protected abstract T createRandomInstance();
    protected abstract T mutateInstance(T instance);
    protected abstract T copyInstance(T instance);

    public void testHashcodeAndEquals() {
        for (int runs = 0; runs < NUMBER_OF_TEST_RUNS; runs++) {
            EqualsHashCodeTestUtils.checkEqualsAndHashCode(createRandomInstance(), this::copyInstance, this::mutateInstance);
        }
    }

    public static StepKey randomStepKey() {
        return new StepKey(randomAlphaOfLength(10), randomAlphaOfLength(10), randomAlphaOfLength(10));
    }

    public void testStepNameNotError() {
        T instance = createRandomInstance();
        StepKey stepKey = instance.getKey();
        assertFalse(ErrorStep.NAME.equals(stepKey.getName()));
        StepKey nextStepKey = instance.getKey();
        assertFalse(ErrorStep.NAME.equals(nextStepKey.getName()));
    }
}
