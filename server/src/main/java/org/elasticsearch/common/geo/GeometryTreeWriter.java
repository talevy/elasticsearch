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

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.geo.geometry.Circle;
import org.elasticsearch.geo.geometry.Geometry;
import org.elasticsearch.geo.geometry.GeometryCollection;
import org.elasticsearch.geo.geometry.GeometryVisitor;
import org.elasticsearch.geo.geometry.Line;
import org.elasticsearch.geo.geometry.LinearRing;
import org.elasticsearch.geo.geometry.MultiLine;
import org.elasticsearch.geo.geometry.MultiPoint;
import org.elasticsearch.geo.geometry.MultiPolygon;
import org.elasticsearch.geo.geometry.Point;
import org.elasticsearch.geo.geometry.Polygon;
import org.elasticsearch.geo.geometry.Rectangle;
import org.elasticsearch.geo.geometry.ShapeType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * This is a tree-writer that serializes the
 * appropriate tree structure for each type of
 * {@link Geometry} into a byte array.
 */
public class GeometryTreeWriter implements Writeable {

    private final GeometryTreeBuilder builder;

    GeometryTreeWriter(Geometry geometry) {
        builder = new GeometryTreeBuilder();
        geometry.visit(builder);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // only write a geometry extent for the tree if the tree
        // contains multiple sub-shapes
        boolean prependExtent = builder.shapeWriters.size() > 1;
        out.writeBoolean(prependExtent);
        if (prependExtent) {
            out.writeInt(builder.minLon);
            out.writeInt(builder.minLat);
            out.writeInt(builder.maxLon);
            out.writeInt(builder.maxLat);
        }
        out.writeVInt(builder.shapeWriters.size());
        for (EdgeTreeWriter writer : builder.shapeWriters) {
            out.writeEnum(ShapeType.POLYGON);
            writer.writeTo(out);
        }
    }

    class GeometryTreeBuilder implements GeometryVisitor<Void, RuntimeException> {

        private List<EdgeTreeWriter> shapeWriters;
        // integers are used to represent int-encoded lat/lon values
        int minLat;
        int maxLat;
        int minLon;
        int maxLon;

        GeometryTreeBuilder() {
            shapeWriters = new ArrayList<>();
            minLat = minLon = Integer.MAX_VALUE;
            maxLat = maxLon = Integer.MIN_VALUE;
        }

        private void addWriter(EdgeTreeWriter writer) {
            minLon = Math.min(minLon, writer.extent.minX);
            minLat = Math.min(minLat, writer.extent.minY);
            maxLon = Math.max(maxLon, writer.extent.maxX);
            maxLat = Math.max(maxLat, writer.extent.maxY);
            shapeWriters.add(writer);
        }

        @Override
        public Void visit(GeometryCollection<?> collection) {
            for (Geometry geometry : collection) {
                geometry.visit(this);
            }
            return null;
        }

        @Override
        public Void visit(Line line) {
            addWriter(new EdgeTreeWriter(asIntArray(line.getLons()), asIntArray(line.getLats())));
            return null;
        }

        @Override
        public Void visit(MultiLine multiLine) {
            for (Line line : multiLine) {
                visit(line);
            }
            return null;
        }

        @Override
        public Void visit(Polygon polygon) {
            // TODO (support holes)
            LinearRing outerShell = polygon.getPolygon();
            addWriter(new EdgeTreeWriter(asIntArray(outerShell.getLons()), asIntArray(outerShell.getLats())));
            return null;
        }

        @Override
        public Void visit(MultiPolygon multiPolygon) {
            for (Polygon polygon : multiPolygon) {
                visit(polygon);
            }
            return null;
        }

        @Override
        public Void visit(Rectangle r) {
            int[] lats = new int[] { (int) r.getMinLat(), (int) r.getMinLat(), (int) r.getMaxLat(), (int) r.getMaxLat(),
                (int) r.getMinLat()};
            int[] lons = new int[] { (int) r.getMinLon(), (int) r.getMaxLon(), (int) r.getMaxLon(), (int) r.getMinLon(),
                (int) r.getMinLon()};
            addWriter(new EdgeTreeWriter(lons, lats));
            return null;
        }

        @Override
        public Void visit(Point point) {
            throw new UnsupportedOperationException("support for Point is a TODO");
        }

        @Override
        public Void visit(MultiPoint multiPoint) {
            throw new UnsupportedOperationException("support for MultiPoint is a TODO");
        }

        @Override
        public Void visit(LinearRing ring) {
            throw new IllegalArgumentException("invalid shape type found [LinearRing]");
        }

        @Override
        public Void visit(Circle circle) {
            throw new IllegalArgumentException("invalid shape type found [Circle]");
        }

        private int[] asIntArray(double[] doub) {
            int[] intArr = new int[doub.length];
            for (int i = 0; i < intArr.length; i++) {
                intArr[i] = (int) doub[i];
            }
            return intArr;
        }
    }
}
