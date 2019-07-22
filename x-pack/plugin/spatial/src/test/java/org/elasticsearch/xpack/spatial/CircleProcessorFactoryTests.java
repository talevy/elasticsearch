/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.spatial;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.test.ESTestCase;

import org.junit.Before;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class CircleProcessorFactoryTests extends ESTestCase {

    private CircleProcessor.Factory factory;

    @Before
    public void init() {
        factory = new CircleProcessor.Factory();
    }

    public void testCreate() {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        String processorTag = randomAlphaOfLength(10);
        CircleProcessor processor = factory.create(null, processorTag, config);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getField(), equalTo("field1"));
        assertThat(processor.getTargetField(), equalTo("field1"));
        assertThat(processor.getErrorInMeters(), equalTo(CircleProcessor.Factory.DEFAULT_ERROR));
        assertThat(processor.getMaxSides(), equalTo(CircleProcessor.Factory.DEFAULT_MAX_SIDES));
    }

    public void testCreateMissingField() {
        Map<String, Object> config = new HashMap<>();
        String processorTag = randomAlphaOfLength(10);
        ElasticsearchParseException e = expectThrows(ElasticsearchParseException.class, () -> factory.create(null, processorTag, config));
        assertThat(e.getMessage(), equalTo("[field] required property is missing"));
    }

    public void testCreateWithTargetField() {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        config.put("target_field", "other");
        String processorTag = randomAlphaOfLength(10);
        CircleProcessor processor = factory.create(null, processorTag, config);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getField(), equalTo("field1"));
        assertThat(processor.getTargetField(), equalTo("other"));
        assertThat(processor.getErrorInMeters(), equalTo(CircleProcessor.Factory.DEFAULT_ERROR));
        assertThat(processor.getMaxSides(), equalTo(CircleProcessor.Factory.DEFAULT_MAX_SIDES));
    }

    public void testCreateWithErrorDefined() {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        config.put("error_in_meters", 10);
        String processorTag = randomAlphaOfLength(10);
        CircleProcessor processor = factory.create(null, processorTag, config);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getField(), equalTo("field1"));
        assertThat(processor.getTargetField(), equalTo("field1"));
        assertThat(processor.getErrorInMeters(), equalTo(10.0));
        assertThat(processor.getMaxSides(), equalTo(CircleProcessor.Factory.DEFAULT_MAX_SIDES));
    }

    public void testCreateWithMaxSidesDefined() {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "field1");
        config.put("max_sides", 10);
        String processorTag = randomAlphaOfLength(10);
        CircleProcessor processor = factory.create(null, processorTag, config);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getField(), equalTo("field1"));
        assertThat(processor.getTargetField(), equalTo("field1"));
        assertThat(processor.getErrorInMeters(), equalTo(CircleProcessor.Factory.DEFAULT_ERROR));
        assertThat(processor.getMaxSides(), equalTo(10));
    }
}
