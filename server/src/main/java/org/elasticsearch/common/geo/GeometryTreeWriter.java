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
import org.elasticsearch.geometry.Circle;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.GeometryCollection;
import org.elasticsearch.geometry.GeometryVisitor;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.MultiLine;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.MultiPolygon;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.Rectangle;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * This is a tree-writer that serializes the
 * appropriate tree structure for each type of
 * {@link Geometry} into a byte array.
 */
public class GeometryTreeWriter implements Writeable {

    private final GeometryTreeBuilder builder;
    private final CoordinateEncoder coordinateEncoder;
    private double centroidX;
    private double centroidY;

    public GeometryTreeWriter(Geometry geometry, CoordinateEncoder coordinateEncoder) {
        this.coordinateEncoder = coordinateEncoder;
        builder = new GeometryTreeBuilder(coordinateEncoder);
        geometry.visit(builder);
        int numShapes = builder.shapeWriters.size();
        centroidX = builder.sumX / numShapes;
        centroidY = builder.sumY / numShapes;
    }

    public Extent extent() {
        return new Extent(builder.top, builder.bottom, builder.negLeft, builder.negRight, builder.posLeft, builder.posRight);
    }

    public double centroidX() {
        return centroidX;
    }

    public double centroidY() {
        return centroidY;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // only write a geometry extent for the tree if the tree
        // contains multiple sub-shapes
        boolean prependExtent = builder.shapeWriters.size() > 1;
        Extent extent = null;
        out.writeInt(coordinateEncoder.encodeX(centroidX));
        out.writeInt(coordinateEncoder.encodeY(centroidY));
        if (prependExtent) {
            extent = new Extent(builder.top, builder.bottom, builder.negLeft, builder.negRight, builder.posLeft, builder.posRight);
        }
        out.writeOptionalWriteable(extent);
        out.writeVInt(builder.shapeWriters.size());
        for (ShapeTreeWriter writer : builder.shapeWriters) {
            out.writeEnum(writer.getShapeType());
            writer.writeTo(out);
        }
    }

    class GeometryTreeBuilder implements GeometryVisitor<Void, RuntimeException> {

        private List<ShapeTreeWriter> shapeWriters;
        private final CoordinateEncoder coordinateEncoder;
        // integers are used to represent int-encoded lat/lon values
        int top = Integer.MIN_VALUE;
        int bottom = Integer.MAX_VALUE;
        int negLeft = Integer.MAX_VALUE;
        int negRight = Integer.MIN_VALUE;
        int posLeft = Integer.MAX_VALUE;
        int posRight = Integer.MIN_VALUE;

        double sumX = 0;
        double sumY = 0;

        GeometryTreeBuilder(CoordinateEncoder coordinateEncoder) {
            this.coordinateEncoder = coordinateEncoder;
            this.shapeWriters = new ArrayList<>();
        }

        private void addWriter(ShapeTreeWriter writer) {
            Extent extent = writer.getExtent();
            top = Math.max(top, extent.top);
            bottom = Math.min(bottom, extent.bottom);
            negLeft = Math.min(negLeft, extent.negLeft);
            negRight = Math.max(negRight, extent.negRight);
            posLeft = Math.min(posLeft, extent.posLeft);
            posRight = Math.max(posRight, extent.posRight);
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
            addWriter(new EdgeTreeWriter(line.getLons(), line.getLats(), coordinateEncoder));
            return null;
        }

        @Override
        public Void visit(MultiLine multiLine) {
            int size = multiLine.size();
            List<double[]> x = new ArrayList<>(size);
            List<double[]> y = new ArrayList<>(size);
            for (Line line : multiLine) {
                x.add(line.getLons());
                y.add(line.getLats());
            }
            addWriter(new EdgeTreeWriter(x, y, coordinateEncoder));
            return null;
        }

        @Override
        public Void visit(Polygon polygon) {
            LinearRing outerShell = polygon.getPolygon();
            int numHoles = polygon.getNumberOfHoles();
            List<double[]> x = new ArrayList<>(numHoles);
            List<double[]> y = new ArrayList<>(numHoles);
            for (int i = 0; i < numHoles; i++) {
                LinearRing innerRing = polygon.getHole(i);
                x.add(innerRing.getLons());
                y.add(innerRing.getLats());
            }
            addWriter(new PolygonTreeWriter(outerShell.getLons(), outerShell.getLats(), x, y, coordinateEncoder));
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
            double[] lats = new double[] { r.getMinLat(), r.getMinLat(), r.getMaxLat(), r.getMaxLat(), r.getMinLat() };
            double[] lons = new double[] { r.getMinLon(), r.getMaxLon(), r.getMaxLon(), r.getMinLon(), r.getMinLon() };
            addWriter(new PolygonTreeWriter(lons, lats, Collections.emptyList(), Collections.emptyList(), coordinateEncoder));
            return null;
        }

        @Override
        public Void visit(Point point) {
            Point2DWriter writer = new Point2DWriter(point.getLon(), point.getLat(), coordinateEncoder);
            addWriter(writer);
            return null;
        }

        @Override
        public Void visit(MultiPoint multiPoint) {
            double[] x = new double[multiPoint.size()];
            double[] y = new double[x.length];
            for (int i = 0; i < multiPoint.size(); i++) {
                x[i] = multiPoint.get(i).getLon();
                y[i] = multiPoint.get(i).getLat();
            }
            Point2DWriter writer = new Point2DWriter(x, y, coordinateEncoder);
            addWriter(writer);
            return null;
        }

        @Override
        public Void visit(LinearRing ring) {
            throw new IllegalArgumentException("invalid shape type found [Circle]");
        }

        @Override
        public Void visit(Circle circle) {
            throw new IllegalArgumentException("invalid shape type found [Circle]");
        }
    }
}
