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
package org.elasticsearch.common.geo;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.geo.geometry.Geometry;
import org.elasticsearch.geo.geometry.Line;
import org.elasticsearch.geo.geometry.LinearRing;
import org.elasticsearch.geo.geometry.MultiPoint;
import org.elasticsearch.geo.geometry.Point;
import org.elasticsearch.geo.geometry.Polygon;
import org.elasticsearch.geo.geometry.Rectangle;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class GeometryTreeTests extends ESTestCase {

    static Extent geoExtent(double minX, double minY, double maxX, double maxY) {
        return Extent.fromPoints(GeoEncodingUtils.encodeLongitude(minX), GeoEncodingUtils.encodeLatitude(minY),
            GeoEncodingUtils.encodeLongitude(maxX), GeoEncodingUtils.encodeLatitude(maxY));
    }

    public void testRectangleShape() throws IOException {
        for (int i = 0; i < 1000; i++) {
            int minX = randomIntBetween(-80, 70);
            int maxX = randomIntBetween(minX + 10, 80);
            int minY = randomIntBetween(-80, 70);
            int maxY = randomIntBetween(minY + 10, 80);
            double[] x = new double[]{minX, maxX, maxX, minX, minX};
            double[] y = new double[]{minY, minY, maxY, maxY, minY};
            Geometry rectangle = randomBoolean() ?
                new Polygon(new LinearRing(y, x), Collections.emptyList()) : new Rectangle(minY, maxY, minX, maxX);
            GeometryTreeWriter writer = new GeometryTreeWriter(rectangle);

            BytesStreamOutput output = new BytesStreamOutput();
            writer.writeTo(output);
            output.close();
            GeometryTreeReader reader = new GeometryTreeReader(output.bytes().toBytesRef());

            assertThat(geoExtent(minX, minY, maxX, maxY), equalTo(reader.getExtent()));

            // box-query touches bottom-left corner
            assertTrue(reader.intersects(geoExtent(minX - randomIntBetween(1, 10), minY - randomIntBetween(1, 10), minX, minY)));
            // box-query touches bottom-right corner
            assertTrue(reader.intersects(geoExtent(maxX, minY - randomIntBetween(1, 10), maxX + randomIntBetween(1, 10), minY)));
            // box-query touches top-right corner
            assertTrue(reader.intersects(geoExtent(maxX, maxY, maxX + randomIntBetween(1, 10), maxY + randomIntBetween(1, 10))));
            // box-query touches top-left corner
            assertTrue(reader.intersects(geoExtent(minX - randomIntBetween(1, 10), maxY, minX, maxY + randomIntBetween(1, 10))));
            // box-query fully-enclosed inside rectangle
            assertTrue(reader.intersects(geoExtent((3 * minX + maxX) / 4, (3 * minY + maxY) / 4, (3 * maxX + minX) / 4,
                (3 * maxY + minY) / 4)));
            // box-query fully-contains poly
            assertTrue(reader.intersects(geoExtent(minX - randomIntBetween(1, 10), minY - randomIntBetween(1, 10),
                maxX + randomIntBetween(1, 10), maxY + randomIntBetween(1, 10))));
            // box-query half-in-half-out-right
            assertTrue(reader.intersects(geoExtent((3 * minX + maxX) / 4, (3 * minY + maxY) / 4, maxX + randomIntBetween(1, 10),
                (3 * maxY + minY) / 4)));
            // box-query half-in-half-out-left
            assertTrue(reader.intersects(geoExtent(minX - randomIntBetween(1, 10), (3 * minY + maxY) / 4, (3 * maxX + minX) / 4,
                (3 * maxY + minY) / 4)));
            // box-query half-in-half-out-top
            assertTrue(reader.intersects(geoExtent((3 * minX + maxX) / 4, (3 * minY + maxY) / 4, maxX + randomIntBetween(1, 10),
                maxY + randomIntBetween(1, 10))));
            // box-query half-in-half-out-bottom
            assertTrue(reader.intersects(geoExtent((3 * minX + maxX) / 4, minY - randomIntBetween(1, 10),
                maxX + randomIntBetween(1, 10), (3 * maxY + minY) / 4)));

            // box-query outside to the right
            assertFalse(reader.intersects(geoExtent(maxX + randomIntBetween(1, 4), minY, maxX + randomIntBetween(5, 10), maxY)));
            // box-query outside to the left
            assertFalse(reader.intersects(geoExtent(maxX - randomIntBetween(5, 10), minY, minX - randomIntBetween(1, 4), maxY)));
            // box-query outside to the top
            assertFalse(reader.intersects(geoExtent(minX, maxY + randomIntBetween(1, 4), maxX, maxY + randomIntBetween(5, 10))));
            // box-query outside to the bottom
            assertFalse(reader.intersects(geoExtent(minX, minY - randomIntBetween(5, 10), maxX, minY - randomIntBetween(1, 4))));
        }
    }

    public void testPacManPolygon() throws Exception {
        // pacman
        double[] px = {0, 10, 10, 0, -8, -10, -8, 0, 10, 10, 0};
        double[] py = {0, 5, 9, 10, 9, 0, -9, -10, -9, -5, 0};

        // test cell crossing poly
        GeometryTreeWriter writer = new GeometryTreeWriter(new Polygon(new LinearRing(py, px), Collections.emptyList()));
        BytesStreamOutput output = new BytesStreamOutput();
        writer.writeTo(output);
        output.close();
        GeometryTreeReader reader = new GeometryTreeReader(output.bytes().toBytesRef());
        assertTrue(reader.intersects(geoExtent(2, -1, 11, 1)));
        assertTrue(reader.intersects(geoExtent(-12, -12, 12, 12)));
        assertTrue(reader.intersects(geoExtent(-2, -1, 2, 0)));
        assertTrue(reader.intersects(geoExtent(-5, -6, 2, -2)));
    }

    // adapted from org.apache.lucene.geo.TestPolygon2D#testMultiPolygon
    public void testPolygonWithHole() throws Exception {
        Polygon polyWithHole = new Polygon(new LinearRing(new double[] { -50, -50, 50, 50, -50 }, new double[] { -50, 50, 50, -50, -50 }),
            Collections.singletonList(new LinearRing(new double[] { -10, -10, 10, 10, -10 }, new double[] { -10, 10, 10, -10, -10 })));

        GeometryTreeWriter writer = new GeometryTreeWriter(polyWithHole);
        BytesStreamOutput output = new BytesStreamOutput();
        writer.writeTo(output);
        output.close();
        GeometryTreeReader reader = new GeometryTreeReader(output.bytes().toBytesRef());

        assertFalse(reader.intersects(geoExtent(6, -6, 6, -6))); // in the hole
        assertTrue(reader.intersects(geoExtent(25, -25, 25, -25))); // on the mainland
        assertFalse(reader.intersects(geoExtent(51, 51, 52, 52))); // outside of mainland
        assertTrue(reader.intersects(geoExtent(-60, -60, 60, 60))); // enclosing us completely
        assertTrue(reader.intersects(geoExtent(49, 49, 51, 51))); // overlapping the mainland
        assertTrue(reader.intersects(geoExtent(9, 9, 11, 11))); // overlapping the hole
    }

    public void testCombPolygon() throws Exception {
        double[] px = {0, 10, 10, 20, 20, 30, 30, 40, 40, 50, 50,  0, 0};
        double[] py = {0, 0,  20, 20,  0,  0, 20, 20,  0,  0, 30, 30, 0};

        double[] hx = {21, 21, 29, 29, 21};
        double[] hy = {1, 20, 20, 1, 1};

        Polygon polyWithHole = new Polygon(new LinearRing(py, px),  Collections.singletonList(new LinearRing(hy, hx)));
        // test cell crossing poly
        GeometryTreeWriter writer = new GeometryTreeWriter(polyWithHole);
        BytesStreamOutput output = new BytesStreamOutput();
        writer.writeTo(output);
        output.close();
        GeometryTreeReader reader = new GeometryTreeReader(output.bytes().toBytesRef());
        assertTrue(reader.intersects(geoExtent(5, 10, 5, 10)));
        assertFalse(reader.intersects(geoExtent(15, 10, 15, 10)));
        assertFalse(reader.intersects(geoExtent(25, 10, 25, 10)));
    }

    public void testPacManClosedLineString() throws Exception {
        // pacman
        double[] px = {0, 10, 10, 0, -8, -10, -8, 0, 10, 10, 0};
        double[] py = {0, 5, 9, 10, 9, 0, -9, -10, -9, -5, 0};

        // test cell crossing poly
        GeometryTreeWriter writer = new GeometryTreeWriter(new Line(px, py));
        BytesStreamOutput output = new BytesStreamOutput();
        writer.writeTo(output);
        output.close();
        GeometryTreeReader reader = new GeometryTreeReader(output.bytes().toBytesRef());
        assertTrue(reader.intersects(geoExtent(2, -1, 11, 1)));
        assertTrue(reader.intersects(geoExtent(-12, -12, 12, 12)));
        assertTrue(reader.intersects(geoExtent(-2, -1, 2, 0)));
        assertFalse(reader.intersects(geoExtent(-5, -6, 2, -2)));
    }

    public void testPacManLineString() throws Exception {
        // pacman
        double[] px = {0, 10, 10, 0, -8, -10, -8, 0, 10, 10};
        double[] py = {0, 5, 9, 10, 9, 0, -9, -10, -9, -5};

        // test cell crossing poly
        GeometryTreeWriter writer = new GeometryTreeWriter(new Line(px, py));
        BytesStreamOutput output = new BytesStreamOutput();
        writer.writeTo(output);
        output.close();
        GeometryTreeReader reader = new GeometryTreeReader(output.bytes().toBytesRef());
        assertTrue(reader.intersects(geoExtent(2, -1, 11, 1)));
        assertTrue(reader.intersects(geoExtent(-12, -12, 12, 12)));
        assertTrue(reader.intersects(geoExtent(-2, -1, 2, 0)));
        assertFalse(reader.intersects(geoExtent(-5, -6, 2, -2)));
    }

    public void testPacManPoints() throws Exception {
        // pacman
        List<Point> points = Arrays.asList(
            new Point(0, 0),
            new Point(5, 10),
            new Point(9, 10),
            new Point(10, 0),
            new Point(9, -8),
            new Point(0, -10),
            new Point(-9, -8),
            new Point(-10, 0),
            new Point(-9, 10),
            new Point(-5, 10)
        );


        // candidate intersects cell
        int xMin = 0;
        int xMax = 11;
        int yMin = -10;
        int yMax = 9;

        // test cell crossing poly
        GeometryTreeWriter writer = new GeometryTreeWriter(new MultiPoint(points));
        BytesStreamOutput output = new BytesStreamOutput();
        writer.writeTo(output);
        output.close();
        GeometryTreeReader reader = new GeometryTreeReader(output.bytes().toBytesRef());
        assertTrue(reader.intersects(geoExtent(xMin, yMin, xMax, yMax)));
    }
}
