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
 * {@link GeometryTreeWriter} and {@link EdgeTreeWriter};
 */
public class Extent implements Writeable {
    static final int WRITEABLE_SIZE_IN_BYTES = 24;

    public final int top;
    public final int bottom;
    public final int negLeft;
    public final int negRight;
    public final int posLeft;
    public final int posRight;

    Extent(int top, int bottom, int negLeft, int negRight, int posLeft, int posRight) {
        this.top = top;
        this.bottom = bottom;
        this.negLeft = negLeft;
        this.negRight = negRight;
        this.posLeft = posLeft;
        this.posRight = posRight;
    }

    static Extent fromPoint(int x, int y) {
        return new Extent(y, y,
            x < 0 ? x : Integer.MAX_VALUE,
            x < 0 ? x : Integer.MIN_VALUE,
            x >= 0 ? x : Integer.MAX_VALUE,
            x >= 0 ? x : Integer.MIN_VALUE);
    }

    static Extent fromPoints(int bottomLeftX, int bottomLeftY, int topRightX, int topRightY) {
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

    Extent(StreamInput input) throws IOException {
        this(input.readInt(), input.readInt(), input.readInt(), input.readInt(), input.readInt(), input.readInt());
    }

    public Extent merge(Extent a, Extent b) {
        return new Extent(Math.max(a.top, b.top), Math.min(a.bottom, b.bottom),
            Math.min(a.negLeft, b.negLeft), Math.max(a.negRight, b.negRight),
            Math.min(a.posLeft, b.posLeft), Math.max(a.posRight, b.posRight));
    }

    public int minY() {
        return bottom;
    }

    public int maxY() {
        return top;
    }

    public int minX() {
        return Math.min(negLeft, posLeft);
    }

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
