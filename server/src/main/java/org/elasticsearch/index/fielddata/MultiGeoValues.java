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

import org.apache.lucene.geo.GeoEncodingUtils;
import org.elasticsearch.common.geo.Extent;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeometryTreeReader;

import java.io.IOException;

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
        private final GeometryTreeReader reader;
        private final Extent extent;

        public GeoShapeValue(GeometryTreeReader reader) throws IOException {
            this.reader = reader;
            this.extent = reader.getExtent();
        }

        public GeoShapeValue() {
            this.reader = null;
            this.extent = null;
        }

        @Override
        public BoundingBox boundingBox() {
            return new BoundingBox(extent);
        }

        @Override
        public double lat() {
            throw new UnsupportedOperationException("centroid of GeoShape is not defined");
        }

        @Override
        public double lon() {
            throw new UnsupportedOperationException("centroid of GeoShape is not defined");
        }
    }

    /**
     * interface for geo-shape and geo-point doc-values to
     * retrieve properties used in aggregations.
     */
    public interface GeoValue {
        BoundingBox boundingBox();
        double lat();
        double lon();
    }

    public static class BoundingBox {
        public final double top;
        public final double bottom;
        public final double negLeft;
        public final double negRight;
        public final double posLeft;
        public final double posRight;

        BoundingBox(Extent extent) {
            this.top = GeoEncodingUtils.decodeLatitude(extent.top);
            this.bottom = GeoEncodingUtils.decodeLatitude(extent.bottom);
            if (extent.negLeft == Integer.MAX_VALUE) {
                this.negLeft = Double.POSITIVE_INFINITY;
            } else {
                this.negLeft = GeoEncodingUtils.decodeLongitude(extent.negLeft);
            }
            if (extent.negRight == Integer.MIN_VALUE) {
                this.negRight = Double.NEGATIVE_INFINITY;
            } else {
                this.negRight = GeoEncodingUtils.decodeLongitude(extent.negRight);
            }
            if (extent.posLeft == Integer.MAX_VALUE) {
                this.posLeft = Double.POSITIVE_INFINITY;
            } else {
                this.posLeft = GeoEncodingUtils.decodeLongitude(extent.posLeft);
            }
            if (extent.posRight == Integer.MIN_VALUE) {
                this.posRight = Double.NEGATIVE_INFINITY;
            } else {
                this.posRight = GeoEncodingUtils.decodeLongitude(extent.posRight);
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

        public GeoPoint topLeft() {
            if (Double.isInfinite(top)) {
                return null;
            } else if (Double.isInfinite(posLeft)) {
                return new GeoPoint(top, negLeft);
            } else if (Double.isInfinite(negLeft)) {
                return new GeoPoint(top, posLeft);
            } else {
                double unwrappedWidth = posRight - negLeft;
                double wrappedWidth = (180 - posLeft) - (-180 - negRight);
                if (unwrappedWidth <= wrappedWidth) {
                    return new GeoPoint(top, negLeft);
                } else {
                    return new GeoPoint(top, posLeft);
                }
            }
        }

        public GeoPoint bottomRight() {
            if (Double.isInfinite(top)) {
                return null;
            } else if (Double.isInfinite(posLeft)) {
                return new GeoPoint(bottom, negRight);
            } else if (Double.isInfinite(negLeft)) {
                return new GeoPoint(bottom, posRight);
            } else {
                double unwrappedWidth = posRight - negLeft;
                double wrappedWidth = (180 - posLeft) - (-180 - negRight);
                if (unwrappedWidth <= wrappedWidth) {
                    return new GeoPoint(bottom, posRight);
                } else {
                    return new GeoPoint(bottom, negRight);
                }
            }
        }
    }
}
