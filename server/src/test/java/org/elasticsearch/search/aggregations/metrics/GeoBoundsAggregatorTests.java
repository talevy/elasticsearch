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

package org.elasticsearch.search.aggregations.metrics;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeometryIndexer;
import org.elasticsearch.common.geo.GeometryParser;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.geo.geometry.Geometry;
import org.elasticsearch.geo.geometry.MultiPoint;
import org.elasticsearch.geo.geometry.Point;
import org.elasticsearch.geo.geometry.Polygon;
import org.elasticsearch.index.mapper.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.GeoShapeFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;
import org.elasticsearch.test.geo.RandomGeoGenerator;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.search.aggregations.metrics.InternalGeoBoundsTests.GEOHASH_TOLERANCE;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
public class GeoBoundsAggregatorTests extends AggregatorTestCase {
    public void testEmptyGeoPoint() throws Exception {
        try (Directory dir = newDirectory();
             RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            GeoBoundsAggregationBuilder aggBuilder = new GeoBoundsAggregationBuilder("my_agg")
                .field("field")
                .wrapLongitude(false);

            MappedFieldType fieldType = new GeoPointFieldMapper.GeoPointFieldType();
            fieldType.setHasDocValues(true);
            fieldType.setName("field");
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalGeoBounds bounds = search(searcher, new MatchAllDocsQuery(), aggBuilder, fieldType);
                assertTrue(Double.isInfinite(bounds.top));
                assertTrue(Double.isInfinite(bounds.bottom));
                assertTrue(Double.isInfinite(bounds.posLeft));
                assertTrue(Double.isInfinite(bounds.posRight));
                assertTrue(Double.isInfinite(bounds.negLeft));
                assertTrue(Double.isInfinite(bounds.negRight));
                assertFalse(AggregationInspectionHelper.hasValue(bounds));
            }
        }
    }

    public void testEmptyGeoShape() throws Exception {
        try (Directory dir = newDirectory();
             RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            GeoBoundsAggregationBuilder aggBuilder = new GeoBoundsAggregationBuilder("my_agg")
                .field("field")
                .wrapLongitude(false);

            MappedFieldType fieldType = new GeoShapeFieldMapper.GeoShapeFieldType();
            fieldType.setHasDocValues(true);
            fieldType.setName("field");
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalGeoBounds bounds = search(searcher, new MatchAllDocsQuery(), aggBuilder, fieldType);
                assertTrue(Double.isInfinite(bounds.top));
                assertTrue(Double.isInfinite(bounds.bottom));
                assertTrue(Double.isInfinite(bounds.posLeft));
                assertTrue(Double.isInfinite(bounds.posRight));
                assertTrue(Double.isInfinite(bounds.negLeft));
                assertTrue(Double.isInfinite(bounds.negRight));
                assertFalse(AggregationInspectionHelper.hasValue(bounds));
            }
        }
    }

    public void testRandomPoints() throws Exception {
        double top = Double.NEGATIVE_INFINITY;
        double bottom = Double.POSITIVE_INFINITY;
        double posLeft = Double.POSITIVE_INFINITY;
        double posRight = Double.NEGATIVE_INFINITY;
        double negLeft = Double.POSITIVE_INFINITY;
        double negRight = Double.NEGATIVE_INFINITY;
        int numDocs = randomIntBetween(50, 100);
        try (Directory dir = newDirectory();
             RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            for (int i = 0; i < numDocs; i++) {
                Document doc = new Document();
                int numValues = randomIntBetween(1, 5);
                for (int j = 0; j < numValues; j++) {
                    GeoPoint point = RandomGeoGenerator.randomPoint(random());
                    if (point.getLat() > top) {
                        top = point.getLat();
                    }
                    if (point.getLat() < bottom) {
                        bottom = point.getLat();
                    }
                    if (point.getLon() >= 0 && point.getLon() < posLeft) {
                        posLeft = point.getLon();
                    }
                    if (point.getLon() >= 0 && point.getLon() > posRight) {
                        posRight = point.getLon();
                    }
                    if (point.getLon() < 0 && point.getLon() < negLeft) {
                        negLeft = point.getLon();
                    }
                    if (point.getLon() < 0 && point.getLon() > negRight) {
                        negRight = point.getLon();
                    }
                    doc.add(new LatLonDocValuesField("field", point.getLat(), point.getLon()));
                }
                w.addDocument(doc);
            }
            GeoBoundsAggregationBuilder aggBuilder = new GeoBoundsAggregationBuilder("my_agg")
                .field("field")
                .wrapLongitude(false);

            MappedFieldType fieldType = new GeoPointFieldMapper.GeoPointFieldType();
            fieldType.setHasDocValues(true);
            fieldType.setName("field");
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalGeoBounds bounds = search(searcher, new MatchAllDocsQuery(), aggBuilder, fieldType);
                assertThat(bounds.top, closeTo(top, GEOHASH_TOLERANCE));
                assertThat(bounds.bottom, closeTo(bottom, GEOHASH_TOLERANCE));
                assertThat(bounds.posLeft, closeTo(posLeft, GEOHASH_TOLERANCE));
                assertThat(bounds.posRight, closeTo(posRight, GEOHASH_TOLERANCE));
                assertThat(bounds.negRight, closeTo(negRight, GEOHASH_TOLERANCE));
                assertThat(bounds.negLeft, closeTo(negLeft, GEOHASH_TOLERANCE));
                assertTrue(AggregationInspectionHelper.hasValue(bounds));
            }
        }
    }

    public void testRandomShapes() throws Exception {
        double top = Double.NEGATIVE_INFINITY;
        double bottom = Double.POSITIVE_INFINITY;
        double posLeft = Double.POSITIVE_INFINITY;
        double posRight = Double.NEGATIVE_INFINITY;
        double negLeft = Double.POSITIVE_INFINITY;
        double negRight = Double.NEGATIVE_INFINITY;
        int numDocs = randomIntBetween(50, 100);
        try (Directory dir = newDirectory();
             RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            for (int i = 0; i < numDocs; i++) {
                Document doc = new Document();
                int numValues = randomIntBetween(1, 5);
                List<Point> points = new ArrayList<>();
                for (int j = 0; j < numValues; j++) {
                    GeoPoint point = RandomGeoGenerator.randomPoint(random());
                    points.add(new Point(point.getLat(), point.getLon()));
                    if (point.getLat() > top) {
                        top = point.getLat();
                    }
                    if (point.getLat() < bottom) {
                        bottom = point.getLat();
                    }
                    if (point.getLon() >= 0 && point.getLon() < posLeft) {
                        posLeft = point.getLon();
                    }
                    if (point.getLon() >= 0 && point.getLon() > posRight) {
                        posRight = point.getLon();
                    }
                    if (point.getLon() < 0 && point.getLon() < negLeft) {
                        negLeft = point.getLon();
                    }
                    if (point.getLon() < 0 && point.getLon() > negRight) {
                        negRight = point.getLon();
                    }
                }
                doc.add(new GeoShapeFieldMapper.BinaryGeoShapeDocValuesField("field", new MultiPoint(points)));
                w.addDocument(doc);
            }
            GeoBoundsAggregationBuilder aggBuilder = new GeoBoundsAggregationBuilder("my_agg")
                .field("field")
                .wrapLongitude(false);

            MappedFieldType fieldType = new GeoShapeFieldMapper.GeoShapeFieldType();
            fieldType.setHasDocValues(true);
            fieldType.setName("field");
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalGeoBounds bounds = search(searcher, new MatchAllDocsQuery(), aggBuilder, fieldType);
                assertThat(bounds.top, closeTo(top, GEOHASH_TOLERANCE));
                assertThat(bounds.bottom, closeTo(bottom, GEOHASH_TOLERANCE));
                assertThat(bounds.posLeft, closeTo(posLeft, GEOHASH_TOLERANCE));
                assertThat(bounds.posRight, closeTo(posRight, GEOHASH_TOLERANCE));
                assertThat(bounds.negRight, closeTo(negRight, GEOHASH_TOLERANCE));
                assertThat(bounds.negLeft, closeTo(negLeft, GEOHASH_TOLERANCE));
                assertTrue(AggregationInspectionHelper.hasValue(bounds));
            }
        }
    }

    public void testFiji() throws Exception {
        XContentParser fijiParser = XContentHelper.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            new BytesArray("{\n" +
            "  \"type\": \"MultiPolygon\",\n" +
            "  \"coordinates\": [\n" +
            "   [\n" +
            "    [\n" +
            "     [\n" +
            "      178.3736,\n" +
            "      -17.33992\n" +
            "     ],\n" +
            "     [\n" +
            "      178.71806,\n" +
            "      -17.62846\n" +
            "     ],\n" +
            "     [\n" +
            "      178.55271,\n" +
            "      -18.15059\n" +
            "     ],\n" +
            "     [\n" +
            "      177.93266,\n" +
            "      -18.28799\n" +
            "     ],\n" +
            "     [\n" +
            "      177.38146,\n" +
            "      -18.16432\n" +
            "     ],\n" +
            "     [\n" +
            "      177.28504,\n" +
            "      -17.72465\n" +
            "     ],\n" +
            "     [\n" +
            "      177.67087,\n" +
            "      -17.38114\n" +
            "     ],\n" +
            "     [\n" +
            "      178.12557,\n" +
            "      -17.50481\n" +
            "     ],\n" +
            "     [\n" +
            "      178.3736,\n" +
            "      -17.33992\n" +
            "     ]\n" +
            "    ]\n" +
            "   ],\n" +
            "   [\n" +
            "    [\n" +
            "     [\n" +
            "      179.364143,\n" +
            "      -16.801354\n" +
            "     ],\n" +
            "     [\n" +
            "      178.725059,\n" +
            "      -17.012042\n" +
            "     ],\n" +
            "     [\n" +
            "      178.596839,\n" +
            "      -16.63915\n" +
            "     ],\n" +
            "     [\n" +
            "      179.096609,\n" +
            "      -16.433984\n" +
            "     ],\n" +
            "     [\n" +
            "      179.413509,\n" +
            "      -16.379054\n" +
            "     ],\n" +
            "     [\n" +
            "      180,\n" +
            "      -16.067133\n" +
            "     ],\n" +
            "     [\n" +
            "      180,\n" +
            "      -16.555217\n" +
            "     ],\n" +
            "     [\n" +
            "      179.364143,\n" +
            "      -16.801354\n" +
            "     ]\n" +
            "    ]\n" +
            "   ],\n" +
            "   [\n" +
            "    [\n" +
            "     [\n" +
            "      -179.917369,\n" +
            "      -16.501783\n" +
            "     ],\n" +
            "     [\n" +
            "      -180,\n" +
            "      -16.555217\n" +
            "     ],\n" +
            "     [\n" +
            "      -180,\n" +
            "      -16.067133\n" +
            "     ],\n" +
            "     [\n" +
            "      -179.79332,\n" +
            "      -16.020882\n" +
            "     ],\n" +
            "     [\n" +
            "      -179.917369,\n" +
            "      -16.501783\n" +
            "     ]\n" +
            "    ]\n" +
            "   ]\n" +
            "  ]\n" +
            " }"), XContentType.JSON);


        XContentParser israelParser = XContentHelper.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            new BytesArray("{\n" +
                "  \"type\": \"Polygon\",\n" +
                "  \"coordinates\": [\n" +
                "   [\n" +
                "    [\n" +
                "     35.719918,\n" +
                "     32.709192\n" +
                "    ],\n" +
                "    [\n" +
                "     35.545665,\n" +
                "     32.393992\n" +
                "    ],\n" +
                "    [\n" +
                "     35.18393,\n" +
                "     32.532511\n" +
                "    ],\n" +
                "    [\n" +
                "     34.974641,\n" +
                "     31.866582\n" +
                "    ],\n" +
                "    [\n" +
                "     35.225892,\n" +
                "     31.754341\n" +
                "    ],\n" +
                "    [\n" +
                "     34.970507,\n" +
                "     31.616778\n" +
                "    ],\n" +
                "    [\n" +
                "     34.927408,\n" +
                "     31.353435\n" +
                "    ],\n" +
                "    [\n" +
                "     35.397561,\n" +
                "     31.489086\n" +
                "    ],\n" +
                "    [\n" +
                "     35.420918,\n" +
                "     31.100066\n" +
                "    ],\n" +
                "    [\n" +
                "     34.922603,\n" +
                "     29.501326\n" +
                "    ],\n" +
                "    [\n" +
                "     34.265433,\n" +
                "     31.219361\n" +
                "    ],\n" +
                "    [\n" +
                "     34.556372,\n" +
                "     31.548824\n" +
                "    ],\n" +
                "    [\n" +
                "     34.488107,\n" +
                "     31.605539\n" +
                "    ],\n" +
                "    [\n" +
                "     34.752587,\n" +
                "     32.072926\n" +
                "    ],\n" +
                "    [\n" +
                "     34.955417,\n" +
                "     32.827376\n" +
                "    ],\n" +
                "    [\n" +
                "     35.098457,\n" +
                "     33.080539\n" +
                "    ],\n" +
                "    [\n" +
                "     35.126053,\n" +
                "     33.0909\n" +
                "    ],\n" +
                "    [\n" +
                "     35.460709,\n" +
                "     33.08904\n" +
                "    ],\n" +
                "    [\n" +
                "     35.552797,\n" +
                "     33.264275\n" +
                "    ],\n" +
                "    [\n" +
                "     35.821101,\n" +
                "     33.277426\n" +
                "    ],\n" +
                "    [\n" +
                "     35.836397,\n" +
                "     32.868123\n" +
                "    ],\n" +
                "    [\n" +
                "     35.700798,\n" +
                "     32.716014\n" +
                "    ],\n" +
                "    [\n" +
                "     35.719918,\n" +
                "     32.709192\n" +
                "    ]\n" +
                "   ]\n" +
                "  ]" +
                "}"), XContentType.JSON);

        fijiParser.nextToken();
        israelParser.nextToken();
        Polygon fiji = (Polygon) new GeometryParser(true, true, true).parse(israelParser);
        Geometry geometry = new GeometryIndexer(true).prepareForIndexing(fiji);

        try (Directory dir = newDirectory();
             RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            Document doc = new Document();
            doc.add(new GeoShapeFieldMapper.BinaryGeoShapeDocValuesField("fiji_shape", geometry));
//            for (Polygon poly : fiji) {
//                for (int i = 0; i < poly.getPolygon().length(); i++) {
//                    doc.add(new LatLonDocValuesField("fiji_points", poly.getPolygon().getLat(i), poly.getPolygon().getLon(i)));
//                }
//            }
            for (int i = 0; i < fiji.getPolygon().length(); i++) {
                doc.add(new LatLonDocValuesField("fiji_points", fiji.getPolygon().getLat(i), fiji.getPolygon().getLon(i)));
            }

            w.addDocument(doc);

            GeoBoundsAggregationBuilder pointsAggBuilder = new GeoBoundsAggregationBuilder("my_agg")
                .field("fiji_points").wrapLongitude(true);
            MappedFieldType pointsType = new GeoPointFieldMapper.GeoPointFieldType();
            pointsType.setHasDocValues(true);
            pointsType.setName("fiji_points");

            GeoBoundsAggregationBuilder shapesAggBuilder = new GeoBoundsAggregationBuilder("my_agg")
                .field("fiji_shape").wrapLongitude(true);
            MappedFieldType shapeType = new GeoShapeFieldMapper.GeoShapeFieldType();
            shapeType.setHasDocValues(true);
            shapeType.setName("fiji_shape");

            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalGeoBounds pointBounds = search(searcher, new MatchAllDocsQuery(), pointsAggBuilder, pointsType);
                InternalGeoBounds shapeBounds = search(searcher, new MatchAllDocsQuery(), shapesAggBuilder, shapeType);
                assertTrue(AggregationInspectionHelper.hasValue(pointBounds));
                assertTrue(AggregationInspectionHelper.hasValue(shapeBounds));
                assertThat(shapeBounds, equalTo(pointBounds));
            }
        }
    }
}
