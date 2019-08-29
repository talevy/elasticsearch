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
package org.elasticsearch.index.fielddata;

import org.elasticsearch.common.geo.CoordinateEncoder;
import org.elasticsearch.common.geo.Extent;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoShapeCoordinateEncoder;
import org.elasticsearch.common.geo.GeometryTreeReader;
import org.elasticsearch.common.geo.GeometryTreeWriter;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.utils.GeographyValidator;
import org.elasticsearch.geometry.utils.WellKnownText;

import java.io.IOException;
import java.text.ParseException;

/**
 * A stateful lightweight per document set of geo values.
 * To iterate over values in a document use the following pattern:
 * <pre>
 *   MultiGeoValues values = ..;
 *   values.setDocId(docId);
 *   final int numValues = values.count();
 *   for (int i = 0; i &lt; numValues; i++) {
 *       GeoValue value = values.valueAt(i);
 *       // process value
 *   }
 * </pre>
 * The set of values associated with a document might contain duplicates and
 * comes in a non-specified order.
 */
public abstract class MultiGeoValues {

    /**
     * Creates a new {@link MultiGeoValues} instance
     */
    protected MultiGeoValues() {
    }

    /**
     * Advance this instance to the given document id
     * @return true if there is a value for this document
     */
    public abstract boolean advanceExact(int doc) throws IOException;

    /**
     * Return the number of geo points the current document has.
     */
    public abstract int docValueCount();

    /**
     * Return the next value associated with the current document. This must not be
     * called more than {@link #docValueCount()} times.
     *
     * Note: the returned {@link GeoValue} might be shared across invocations.
     *
     * @return the next value for the current docID set to {@link #advanceExact(int)}.
     */
    public abstract GeoValue nextValue() throws IOException;

    public static class GeoPointValue implements GeoValue {
        private final GeoPoint geoPoint;

        public GeoPointValue(GeoPoint geoPoint) {
            this.geoPoint = geoPoint;
        }

        public GeoPoint geoPoint() {
            return geoPoint;
        }

        @Override
        public BoundingBox boundingBox() {
            return new BoundingBox(geoPoint);
        }

        @Override
        public double lat() {
            return geoPoint.lat();
        }

        @Override
        public double lon() {
            return geoPoint.lon();
        }

        @Override
        public String toString() {
            return geoPoint.toString();
        }
    }

    public static class GeoShapeValue implements GeoValue {
        private static final WellKnownText MISSING_GEOMETRY_PARSER = new WellKnownText(true, new GeographyValidator(true));

        private final GeometryTreeReader reader;
        private final Extent extent;

        public GeoShapeValue(GeometryTreeReader reader) throws IOException {
            this.reader = reader;
            this.extent = reader.getExtent();
        }

        public GeoShapeValue(Extent extent) {
            this.reader = null;
            this.extent = extent;
        }

        @Override
        public BoundingBox boundingBox() {
            return new BoundingBox(extent, GeoShapeCoordinateEncoder.INSTANCE);
        }

        @Override
        public double lat() {
            try {
                return GeoShapeCoordinateEncoder.INSTANCE.decodeY(reader.getCentroidY());
            } catch (IOException e) {
                throw new IllegalStateException("unable to read centroid of shape", e);
            }
        }

        @Override
        public double lon() {
            try {
                return GeoShapeCoordinateEncoder.INSTANCE.decodeX(reader.getCentroidX());
            } catch (IOException e) {
                throw new IllegalStateException("unable to read centroid of shape", e);
            }
        }

        public static GeoShapeValue missing(String missing) {
            try {
                Geometry geometry = MISSING_GEOMETRY_PARSER.fromWKT(missing);
                GeometryTreeWriter writer = new GeometryTreeWriter(geometry, GeoShapeCoordinateEncoder.INSTANCE);
                return new GeoShapeValue(writer.extent());
            } catch (IOException | ParseException e) {
                throw new IllegalArgumentException("Can't apply missing value [" + missing + "]", e);
            }
        }
    }

    /**
     * interface for geo-shape and geo-point doc-values to
     * retrieve properties used in aggregations.
     */
    public interface GeoValue {
        double lat();
        double lon();
        BoundingBox boundingBox();
    }

    public static class BoundingBox {
        public final double top;
        public final double bottom;
        public final double negLeft;
        public final double negRight;
        public final double posLeft;
        public final double posRight;

        BoundingBox(Extent extent, CoordinateEncoder coordinateEncoder) {
            this.top = coordinateEncoder.decodeY(extent.top);
            this.bottom = coordinateEncoder.decodeY(extent.bottom);
            if (extent.negLeft == Integer.MAX_VALUE) {
                this.negLeft = Double.POSITIVE_INFINITY;
            } else {
                this.negLeft = coordinateEncoder.decodeX(extent.negLeft);
            }
            if (extent.negRight == Integer.MIN_VALUE) {
                this.negRight = Double.NEGATIVE_INFINITY;
            } else {
                this.negRight = coordinateEncoder.decodeX(extent.negRight);
            }
            if (extent.posLeft == Integer.MAX_VALUE) {
                this.posLeft = Double.POSITIVE_INFINITY;
            } else {
                this.posLeft = coordinateEncoder.decodeX(extent.posLeft);
            }
            if (extent.posRight == Integer.MIN_VALUE) {
                this.posRight = Double.NEGATIVE_INFINITY;
            } else {
                this.posRight = coordinateEncoder.decodeX(extent.posRight);
            }
        }

        BoundingBox(GeoPoint point) {
            this.top = point.lat();
            this.bottom = point.lat();
            if (point.lon() < 0) {
                this.negLeft = point.lon();
                this.negRight = point.lon();
                this.posLeft = Double.POSITIVE_INFINITY;
                this.posRight = Double.NEGATIVE_INFINITY;
            } else {
                this.negLeft = Double.POSITIVE_INFINITY;
                this.negRight = Double.NEGATIVE_INFINITY;
                this.posLeft = point.lon();
                this.posRight = point.lon();
            }
        }
    }
}
