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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Objects;

/**
 * Object representing the extent of a geometry object within a
 * {@link GeometryTreeWriter} and {@link EdgeTreeWriter}.
 */
public class Extent implements Writeable {
    static final int WRITEABLE_SIZE_IN_BYTES = 24;

    public final int top;
    public final int bottom;
    public final int negLeft;
    public final int negRight;
    public final int posLeft;
    public final int posRight;

    public Extent(int top, int bottom, int negLeft, int negRight, int posLeft, int posRight) {
        this.top = top;
        this.bottom = bottom;
        this.negLeft = negLeft;
        this.negRight = negRight;
        this.posLeft = posLeft;
        this.posRight = posRight;
    }

    Extent(StreamInput input) throws IOException {
        this(input.readInt(), input.readInt(), input.readInt(), input.readInt(), input.readInt(), input.readInt());
    }

    /**
     * calculates the extent of a point, which is the point itself.
     * @param x the x-coordinate of the point
     * @param y the y-coordinate of the point
     * @return the extent of the point
     */
    public static Extent fromPoint(int x, int y) {
        return new Extent(y, y,
            x < 0 ? x : Integer.MAX_VALUE,
            x < 0 ? x : Integer.MIN_VALUE,
            x >= 0 ? x : Integer.MAX_VALUE,
            x >= 0 ? x : Integer.MIN_VALUE);
    }

    /**
     * calculates the extent of two points representing a bounding box's bottom-left
     * and top-right points. It is important that these points accurately represent the
     * bottom-left and top-right of the extent since there is no validation being done.
     *
     * @param bottomLeftX the bottom-left x-coordinate
     * @param bottomLeftY the bottom-left y-coordinate
     * @param topRightX   the top-right x-coordinate
     * @param topRightY   the top-right y-coordinate
     * @return the extent of the two points
     */
    public static Extent fromPoints(int bottomLeftX, int bottomLeftY, int topRightX, int topRightY) {
        int negLeft = Integer.MAX_VALUE;
        int negRight = Integer.MIN_VALUE;
        int posLeft = Integer.MAX_VALUE;
        int posRight = Integer.MIN_VALUE;
        if (bottomLeftX < 0 && topRightX < 0) {
            negLeft = bottomLeftX;
            negRight = topRightX;
        } else if (bottomLeftX < 0) {
            negLeft = negRight = bottomLeftX;
            posLeft = posRight = topRightX;
        } else {
            posLeft = bottomLeftX;
            posRight = topRightX;
        }
        return new Extent(topRightY, bottomLeftY, negLeft, negRight, posLeft, posRight);
    }

    /**
     * @return the minimum y-coordinate of the extent
     */
    public int minY() {
        return bottom;
    }

    /**
     * @return the maximum y-coordinate of the extent
     */
    public int maxY() {
        return top;
    }

    /**
     * @return the absolute minimum x-coordinate of the extent, whether it is positive or negative.
     */
    public int minX() {
        return Math.min(negLeft, posLeft);
    }

    /**
     * @return the absolute maximum x-coordinate of the extent, whether it is positive or negative.
     */
    public int maxX() {
        return Math.max(negRight, posRight);
    }


    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(top);
        out.writeInt(bottom);
        out.writeInt(negLeft);
        out.writeInt(negRight);
        out.writeInt(posLeft);
        out.writeInt(posRight);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Extent extent = (Extent) o;
        return top == extent.top &&
            bottom == extent.bottom &&
            negLeft == extent.negLeft &&
            negRight == extent.negRight &&
            posLeft == extent.posLeft &&
            posRight == extent.posRight;
    }

    @Override
    public int hashCode() {
        return Objects.hash(top, bottom, negLeft, negRight, posLeft, posRight);
    }
}
