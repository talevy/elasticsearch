/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.spatial;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.geo.GeoJson;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.geo.geometry.Circle;
import org.elasticsearch.geo.geometry.Polygon;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.RandomDocumentPicks;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.ingest.IngestDocumentMatcher.assertIngestDocument;
import static org.hamcrest.Matchers.equalTo;

public class CircleProcessorTests extends ESTestCase {

    @SuppressWarnings("unchecked")
    public void testJson() throws IOException {
        Circle circle = new Circle(1.0, 101.0, 10);
        HashMap<String, Object> map = new HashMap<>();
        HashMap<String, Object> circleMap = new HashMap<>();
        circleMap.put("type", "Circle");
        circleMap.put("coordinates", List.of(circle.getLon(), circle.getLat()));
        circleMap.put("radius", circle.getRadiusMeters() + "m");
        map.put("field", circleMap);
        Polygon expectedPoly = CircleProcessor.createRegularPolygon(circle.getLat(), circle.getLon(), circle.getRadiusMeters(), 237);
        IngestDocument ingestDocument = new IngestDocument(map, Collections.emptyMap());
        CircleProcessor processor = new CircleProcessor("tag", "field", "field", false,
            1, CircleProcessor.Factory.DEFAULT_MAX_SIDES);
        processor.execute(ingestDocument);
        Map<String, Object> polyMap = ingestDocument.getFieldValue("field", Map.class);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        GeoJson.toXContent(expectedPoly, builder, ToXContent.EMPTY_PARAMS);
        Tuple<XContentType, Map<String, Object>> expected = XContentHelper.convertToMap(BytesReference.bytes(builder),
            true, XContentType.JSON);
        assertThat(polyMap, equalTo(expected.v2()));
    }

    public void testWKT() {
        Circle circle = new Circle(1.0, 101.0, 10);
        HashMap<String, Object> map = new HashMap<>();
        map.put("field", CircleProcessor.WKT.toWKT(circle));
        Polygon expectedPoly = CircleProcessor.createRegularPolygon(circle.getLat(), circle.getLon(), circle.getRadiusMeters(), 237);
        IngestDocument ingestDocument = new IngestDocument(map, Collections.emptyMap());
        CircleProcessor processor = new CircleProcessor("tag", "field", "field", false,
            1, CircleProcessor.Factory.DEFAULT_MAX_SIDES);
        processor.execute(ingestDocument);
        String polyString = ingestDocument.getFieldValue("field", String.class);
        assertThat(polyString, equalTo(CircleProcessor.WKT.toWKT(expectedPoly)));
    }

    public void testNullValueWithIgnoreMissing() {
        CircleProcessor processor = new CircleProcessor(randomAlphaOfLength(10), "source_field", "target_field",
            true, CircleProcessor.Factory.DEFAULT_ERROR, CircleProcessor.Factory.DEFAULT_MAX_SIDES);
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(random(),
            Collections.singletonMap("source_field", null));
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        processor.execute(ingestDocument);
        assertIngestDocument(originalIngestDocument, ingestDocument);
    }

    public void testNonExistentWithIgnoreMissing() {
        CircleProcessor processor = new CircleProcessor(randomAlphaOfLength(10), "source_field", "target_field",
            true, CircleProcessor.Factory.DEFAULT_ERROR, CircleProcessor.Factory.DEFAULT_MAX_SIDES);
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.emptyMap());
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        processor.execute(ingestDocument);
        assertIngestDocument(originalIngestDocument, ingestDocument);
    }

    public void testNullWithoutIgnoreMissing() {
        CircleProcessor processor = new CircleProcessor(randomAlphaOfLength(10), "source_field", "target_field",
            false, CircleProcessor.Factory.DEFAULT_ERROR, CircleProcessor.Factory.DEFAULT_MAX_SIDES);
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(random(),
            Collections.singletonMap("source_field", null));
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        Exception exception = expectThrows(Exception.class, () -> processor.execute(ingestDocument));
        assertThat(exception.getMessage(), equalTo("field [source_field] is null, cannot extract circle."));
    }

    public void testNonExistentWithoutIgnoreMissing() {
        CircleProcessor processor = new CircleProcessor(randomAlphaOfLength(10), "source_field", "target_field",
            false, CircleProcessor.Factory.DEFAULT_ERROR, CircleProcessor.Factory.DEFAULT_MAX_SIDES);
        IngestDocument originalIngestDocument = RandomDocumentPicks.randomIngestDocument(random(), Collections.emptyMap());
        IngestDocument ingestDocument = new IngestDocument(originalIngestDocument);
        Exception exception = expectThrows(Exception.class, () -> processor.execute(ingestDocument));
        assertThat(exception.getMessage(), equalTo("field [source_field] not present as part of path [source_field]"));
    }

    public void testInvalidWKT() {
        HashMap<String, Object> map = new HashMap<>();
        map.put("field", "invalid");
        IngestDocument ingestDocument = new IngestDocument(map, Collections.emptyMap());
        CircleProcessor processor = new CircleProcessor("tag", "field", "field", false,
            1, CircleProcessor.Factory.DEFAULT_MAX_SIDES);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
        assertThat(e.getMessage(), equalTo("Unknown geometry type: invalid"));
        map.put("field", "POINT (30 10)");
        e = expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
        assertThat(e.getMessage(), equalTo("geometry must be a circle. found [POINT]"));
    }

    public void testMissingField() {
        IngestDocument ingestDocument = new IngestDocument(new HashMap<>(), Collections.emptyMap());
        CircleProcessor processor = new CircleProcessor("tag", "field", "field", false,
            1, CircleProcessor.Factory.DEFAULT_MAX_SIDES);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
        assertThat(e.getMessage(), equalTo("field [field] not present as part of path [field]"));
    }

    public void testInvalidType() {
        Map<String, Object> field = new HashMap<>();
        field.put("coordinates", List.of(100, 100));
        field.put("radius", "10m");
        Map<String, Object> map = new HashMap<>();
        map.put("field", field);
        IngestDocument ingestDocument = new IngestDocument(map, Collections.emptyMap());
        CircleProcessor processor = new CircleProcessor("tag", "field", "field", false,
            1, CircleProcessor.Factory.DEFAULT_MAX_SIDES);

        for (Object value : new Object[] { null, 4.0, "not_circle"}) {
            field.put("type", value);
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
            assertThat(e.getMessage(), equalTo("invalid geometry [{coordinates=[100, 100], radius=10m, type=" + value + "}]"));
        }
    }

    public void testInvalidCoordinates() {
        Map<String, Object> field = new HashMap<>();
        field.put("type", "circle");
        field.put("radius", "10m");
        Map<String, Object> map = new HashMap<>();
        map.put("field", field);
        IngestDocument ingestDocument = new IngestDocument(map, Collections.emptyMap());
        CircleProcessor processor = new CircleProcessor("tag", "field", "field", false,
            1, CircleProcessor.Factory.DEFAULT_MAX_SIDES);

        for (Object value : new Object[] { null, "not_circle"}) {
            field.put("coordinates", value);
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
            assertThat(e.getMessage(), equalTo("invalid geometry [{coordinates=" + value + ", type=circle, radius=10m}]"));
        }
    }

    public void testInvalidRadius() {
        Map<String, Object> field = new HashMap<>();
        field.put("type", "circle");
        field.put("coordinates", List.of(100.0, 1.0));
        Map<String, Object> map = new HashMap<>();
        map.put("field", field);
        IngestDocument ingestDocument = new IngestDocument(map, Collections.emptyMap());
        CircleProcessor processor = new CircleProcessor("tag", "field", "field", false,
            1, CircleProcessor.Factory.DEFAULT_MAX_SIDES);

        for (Object value : new Object[] { null, "NotNumber", "10.0fs"}) {
            field.put("radius", value);
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> processor.execute(ingestDocument));
            assertThat(e.getMessage(), equalTo("invalid geometry [{coordinates=[100.0, 1.0], type=circle, radius=" + value + "}]"));
        }
    }
}
