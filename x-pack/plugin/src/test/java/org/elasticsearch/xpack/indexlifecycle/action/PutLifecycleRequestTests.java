/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle.action;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;
import org.elasticsearch.xpack.indexlifecycle.DeleteAction;
import org.elasticsearch.xpack.indexlifecycle.LifecycleAction;
import org.elasticsearch.xpack.indexlifecycle.LifecyclePolicy;
import org.elasticsearch.xpack.indexlifecycle.Phase;
import org.elasticsearch.xpack.indexlifecycle.action.PutLifecycleAction.Request;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PutLifecycleRequestTests extends AbstractStreamableXContentTestCase<PutLifecycleAction.Request> {
    
    private NamedXContentRegistry registry;
    private String lifecycleName;

    @Before
    public void setup() {
        List<NamedXContentRegistry.Entry> entries = Arrays
                .asList(new NamedXContentRegistry.Entry(LifecycleAction.class, new ParseField(DeleteAction.NAME), DeleteAction::parse));
        registry = new NamedXContentRegistry(entries);
        lifecycleName = randomAlphaOfLength(20); // NOCOMMIT we need to randomise the lifecycle name rather 
                                                 // than use the same name for all instances
    }

    @Override
    protected Request createTestInstance() {
        int numberPhases = 1; // NOCOMMIT need to make this more than one when phase order doesn't rely on JSON map order
        List<Phase> phases = new ArrayList<>(numberPhases);
        for (int i = 0; i < numberPhases; i++) {
            TimeValue after = TimeValue.parseTimeValue(randomTimeValue(0, 1000000000, "s", "m", "h", "d"), "test_after");
            List<LifecycleAction> actions = new ArrayList<>();
            if (randomBoolean()) {
                actions.add(new DeleteAction());
            }
            phases.add(new Phase(randomAlphaOfLength(10), after, actions));
        }
        return new Request(new LifecyclePolicy(lifecycleName, phases));
    }

    @Override
    protected Request createBlankInstance() {
        return new Request();
    }

    @Override
    protected Request doParseInstance(XContentParser parser) {
        return PutLifecycleAction.Request.parseRequest(lifecycleName, parser, registry);
    }

    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
                Arrays.asList(new NamedWriteableRegistry.Entry(LifecycleAction.class, DeleteAction.NAME, DeleteAction::new)));
    }

    protected boolean supportsUnknownFields() {
        return false;
    }

}
