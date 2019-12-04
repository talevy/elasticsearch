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

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.geometry.ShapeType;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A reusable tree reader.
 *
 * This class supports checking bounding box
 * relations against the serialized geometry tree.
 */
public class GeometryTreeReader implements ShapeTreeReader {

    private static final int EXTENT_OFFSET = 8;
    private int startPosition;
    private ByteBufferStreamInput input;
    private final CoordinateEncoder coordinateEncoder;
    // re-usable sub-tree readers
    public PolygonTreeReader polygonTreeReader;
    private EdgeTreeReader edgeTreeReader;
    private Point2DReader point2DReader;

    public GeometryTreeReader(CoordinateEncoder coordinateEncoder) {
        this.coordinateEncoder = coordinateEncoder;
    }

    private GeometryTreeReader(ByteBufferStreamInput input, CoordinateEncoder coordinateEncoder) throws IOException {
        this.input = input;
        startPosition = input.position();
        this.coordinateEncoder = coordinateEncoder;
    }

    public void reset(BytesRef bytesRef) throws IOException {
        this.input = new ByteBufferStreamInput(ByteBuffer.wrap(bytesRef.bytes, bytesRef.offset, bytesRef.length));
        this.startPosition = input.position();
    }

    @Override
    public double getCentroidX() throws IOException {
        input.position(startPosition);
        return coordinateEncoder.decodeX(input.readInt());
    }

    @Override
    public double getCentroidY() throws IOException {
        input.position(startPosition + 4);
        return coordinateEncoder.decodeY(input.readInt());
    }

    @Override
    public Extent getExtent() throws IOException {
        input.position(startPosition + EXTENT_OFFSET);
        Extent extent = input.readOptionalWriteable(Extent::new);
        if (extent != null) {
            return extent;
        }
        int numShapes = input.readVInt();
        assert numShapes == 1;
        int shapeTypeOrdinal = input.readVInt();
        ShapeTreeReader reader = getReader(shapeTypeOrdinal, coordinateEncoder, input);
        return reader.getExtent();
    }

    @Override
    public GeoRelation relate(int minX, int minY, int maxX, int maxY) throws IOException {
        GeoRelation relation = GeoRelation.QUERY_DISJOINT;
        input.position(startPosition + EXTENT_OFFSET);
        boolean hasExtent = input.readBoolean();
        if (hasExtent) {
            int thisMaxY = input.readInt();
            int thisMinY = input.readInt();
            int negLeft = input.readInt();
            int negRight = input.readInt();
            int posLeft = input.readInt();
            int posRight = input.readInt();
            int thisMinX = Math.min(negLeft, posLeft);
            int thisMaxX = Math.max(negRight, posRight);

            // check extent
            if (thisMinY > maxY || thisMaxX < minX || thisMaxY < minY || thisMinX > maxX) {
                return GeoRelation.QUERY_DISJOINT; // tree and bbox-query are disjoint
            }
            if (minX <= thisMinX && minY <= thisMinY && maxX >= thisMaxX && maxY >= thisMaxY) {
                return GeoRelation.QUERY_CROSSES; // bbox-query fully contains tree's
            }
        }

        int numTrees = input.readVInt();
        int nextPosition = input.position();
        for (int i = 0; i < numTrees; i++) {
            if (numTrees > 1) {
                if (i > 0) {
                    input.position(nextPosition);
                }
                int pos = input.readVInt();
                nextPosition = input.position() + pos;
            }
            // read ShapeType ordinal to avoid readEnum allocations
            int shapeTypeOrdinal = input.readVInt();
            ShapeTreeReader reader = getReader(shapeTypeOrdinal, coordinateEncoder, input);
            GeoRelation shapeRelation = reader.relate(minX, minY, maxX, maxY);
            if (GeoRelation.QUERY_CROSSES == shapeRelation ||
                (GeoRelation.QUERY_DISJOINT == shapeRelation && GeoRelation.QUERY_INSIDE == relation)
            ) {
                return GeoRelation.QUERY_CROSSES;
            } else {
                relation = shapeRelation;
            }
        }

        return relation;
    }

    private ShapeTreeReader getReader(int shapeTypeOrdinal, CoordinateEncoder coordinateEncoder, ByteBufferStreamInput input)
            throws IOException {
        if (shapeTypeOrdinal == ShapeType.POLYGON.ordinal()) {
            if (polygonTreeReader == null) {
                polygonTreeReader = new PolygonTreeReader(input);
                return polygonTreeReader;
            }
            polygonTreeReader.reset(input);
            return polygonTreeReader;
        } else if (shapeTypeOrdinal == ShapeType.POINT.ordinal() || shapeTypeOrdinal == ShapeType.MULTIPOINT.ordinal()) {
            if (point2DReader == null) {
                point2DReader = new Point2DReader(input);
                return point2DReader;
            }
            point2DReader.reset(input);
            return point2DReader;
        } else if (shapeTypeOrdinal == ShapeType.LINESTRING.ordinal() || shapeTypeOrdinal == ShapeType.MULTILINESTRING.ordinal()) {
            if (edgeTreeReader == null) {
                edgeTreeReader = new EdgeTreeReader(input, false);
                return edgeTreeReader;
            }
            edgeTreeReader.reset(input, false);
            return edgeTreeReader;
        } else if (shapeTypeOrdinal == ShapeType.GEOMETRYCOLLECTION.ordinal()) {
            return new GeometryTreeReader(input, coordinateEncoder);
        }
        throw new UnsupportedOperationException("unsupported shape type ordinal [" + shapeTypeOrdinal + "]");
    }
}
