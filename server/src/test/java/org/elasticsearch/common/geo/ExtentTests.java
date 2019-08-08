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

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class ExtentTests extends AbstractWireSerializingTestCase {

    public void testFromPoint() {
        int x = randomFrom(-1, 0, 1);
        int y = randomFrom(-1, 0, 1);
        Extent extent = Extent.fromPoint(x, y);
        assertThat(extent.minX(), equalTo(x));
        assertThat(extent.maxX(), equalTo(x));
        assertThat(extent.minY(), equalTo(y));
        assertThat(extent.maxY(), equalTo(y));
        assertThat(extent.centroidX, equalTo(x));
        assertThat(extent.centroidY, equalTo(y));
    }

    public void testFromPoints() {
        int bottomLeftX = randomFrom(-10, 0, 10);
        int bottomLeftY = randomFrom(-10, 0, 10);
        int topRightX = bottomLeftX + randomIntBetween(0, 20);
        int topRightY = bottomLeftX + randomIntBetween(0, 20);
        Extent extent = Extent.fromPoints(bottomLeftX, bottomLeftY, topRightX, topRightY);
        assertThat(extent.minX(), equalTo(bottomLeftX));
        assertThat(extent.maxX(), equalTo(topRightX));
        assertThat(extent.minY(), equalTo(bottomLeftY));
        assertThat(extent.maxY(), equalTo(topRightY));
        assertThat(extent.centroidX, equalTo((bottomLeftX + topRightX) / 2));
        assertThat(extent.centroidY, equalTo((bottomLeftY + topRightY) / 2));
        assertThat(extent.top, equalTo(topRightY));
        assertThat(extent.bottom, equalTo(bottomLeftY));
        if (bottomLeftX < 0 && topRightX < 0) {
            assertThat(extent.negLeft, equalTo(bottomLeftX));
            assertThat(extent.negRight, equalTo(topRightX));
            assertThat(extent.posLeft, equalTo(Integer.MAX_VALUE));
            assertThat(extent.posRight, equalTo(Integer.MIN_VALUE));
        } else if (bottomLeftX < 0) {
            assertThat(extent.negLeft, equalTo(bottomLeftX));
            assertThat(extent.negRight, equalTo(bottomLeftX));
            assertThat(extent.posLeft, equalTo(topRightX));
            assertThat(extent.posRight, equalTo(topRightX));
        } else {
            assertThat(extent.negLeft, equalTo(Integer.MAX_VALUE));
            assertThat(extent.negRight, equalTo(Integer.MIN_VALUE));
            assertThat(extent.posLeft, equalTo(bottomLeftX));
            assertThat(extent.posRight, equalTo(topRightX));
        }
    }

    @Override
    protected Extent createTestInstance() {
        return new Extent(randomIntBetween(-10, 10), randomIntBetween(-10, 10), randomIntBetween(-10, 10),
            randomIntBetween(-10, 10), randomIntBetween(-10, 10), randomIntBetween(-10, 10),
            randomIntBetween(-10, 10), randomIntBetween(-10, 10));
    }

    @Override
    protected Writeable.Reader instanceReader() {
        return Extent::new;
    }

    @Override
    protected Object copyInstance(Object instance, Version version) throws IOException {
        Extent other = (Extent) instance;
        return new Extent(other.top, other.bottom, other.negLeft, other.negRight, other.posLeft, other.posRight,
            other.centroidX, other.centroidY);
    }
}
