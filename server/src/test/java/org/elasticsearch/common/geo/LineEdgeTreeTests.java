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

import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.geo.geometry.Line;
import org.elasticsearch.geo.geometry.Polygon;
import org.elasticsearch.geo.geometry.ShapeType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.geo.RandomShapeGenerator;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.shape.Rectangle;
import org.locationtech.spatial4j.shape.impl.RectangleImpl;

import java.io.IOException;
import java.nio.ByteBuffer;

public class LineEdgeTreeTests extends ESTestCase {

    public void testLine() throws IOException {
        for (int i = 0; i < 1000; i++) {
            int minX = randomIntBetween(-180, 170);
            int maxX = randomIntBetween(minX + 10, 180);
            int minY = randomIntBetween(-180, 170);
            int maxY = randomIntBetween(minY + 10, 180);
            Rectangle rect = new RectangleImpl(minX, maxX, minY, maxY, SpatialContext.GEO);
            ShapeBuilder lineBuilder = RandomShapeGenerator.createShapeWithin(random(), rect, RandomShapeGenerator.ShapeType.LINESTRING);
            Line line = (Line) lineBuilder.buildGeometry();
            int[] x = asIntArray(line.getLons());
            int[] y = asIntArray(line.getLats());
            EdgeTreeWriter writer = new EdgeTreeWriter(x, y);
            BytesStreamOutput output = new BytesStreamOutput();
            writer.writeTo(output);
            output.close();
            EdgeTreeReader reader = new EdgeTreeReader.LineEdgeTreeReader(new ByteBufferStreamInput(ByteBuffer.wrap(output.bytes().toBytesRef().bytes)));

            // box-query fully-enclosed inside rectangle
            assertTrue(reader.containedInOrCrosses((3 * minX + maxX) / 4, (3 * minY + maxY) / 4, (3 * maxX + minX) / 4,
                (3 * maxY + minY) / 4));
            // box-query fully-contains poly
            assertTrue(reader.containedInOrCrosses(minX - randomIntBetween(1, 180), minY - randomIntBetween(1, 180),
                maxX + randomIntBetween(1, 180), maxY + randomIntBetween(1, 180)));
            // box-query half-in-half-out-right
            assertTrue(reader.containedInOrCrosses((3 * minX + maxX) / 4, (3 * minY + maxY) / 4, maxX + randomIntBetween(1, 1000),
                (3 * maxY + minY) / 4));
            // box-query half-in-half-out-left
            assertTrue(reader.containedInOrCrosses(minX - randomIntBetween(1, 1000), (3 * minY + maxY) / 4, (3 * maxX + minX) / 4,
                (3 * maxY + minY) / 4));
            // box-query half-in-half-out-top
            assertTrue(reader.containedInOrCrosses((3 * minX + maxX) / 4, (3 * minY + maxY) / 4, maxX + randomIntBetween(1, 1000),
                maxY + randomIntBetween(1, 1000)));
            // box-query half-in-half-out-bottom
            assertTrue(reader.containedInOrCrosses((3 * minX + maxX) / 4, minY - randomIntBetween(1, 1000),
                maxX + randomIntBetween(1, 1000), (3 * maxY + minY) / 4));

            // box-query outside to the right
            assertFalse(reader.containedInOrCrosses(maxX + randomIntBetween(1, 1000), minY, maxX + randomIntBetween(1001, 2000), maxY));
            // box-query outside to the left
            assertFalse(reader.containedInOrCrosses(maxX - randomIntBetween(1001, 2000), minY, minX - randomIntBetween(1, 1000), maxY));
            // box-query outside to the top
            assertFalse(reader.containedInOrCrosses(minX, maxY + randomIntBetween(1, 1000), maxX, maxY + randomIntBetween(1001, 2000)));
            // box-query outside to the bottom
            assertFalse(reader.containedInOrCrosses(minX, minY - randomIntBetween(1001, 2000), maxX, minY - randomIntBetween(1, 1000)));
        }
    }

    private int[] asIntArray(double[] doub) {
        int[] intArr = new int[doub.length];
        for (int i = 0; i < intArr.length; i++) {
            intArr[i] = (int) doub[i];
        }
        return intArr;
    }
}
