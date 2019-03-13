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
package org.elasticsearch.search.aggregations.bucket.geogrid;

import org.apache.lucene.geo.Polygon2D;
import org.apache.lucene.geo.Polygon2DUtils;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.geo.geometry.Point;
import org.elasticsearch.index.fielddata.AbstractSortingNumericDocValues;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.aggregations.support.ValuesSource;

import java.io.IOException;

import static org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils.MAX_ZOOM;
import static org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils.ZOOM_SHIFT;

/**
 * Wrapper class to help convert {@link MultiGeoPointValues}
 * to numeric long values for bucketing.
 */
class CellIdSource extends ValuesSource.Numeric {
    private final ValuesSource valuesSource;
    private final int precision;
    private final GeoPointLongEncoder encoder;

    CellIdSource(ValuesSource valuesSource, int precision, GeoPointLongEncoder encoder) {
        this.valuesSource = valuesSource;
        //different GeoPoints could map to the same or different hashing cells.
        this.precision = precision;
        this.encoder = encoder;
    }

    public int precision() {
        return precision;
    }

    @Override
    public boolean isFloatingPoint() {
        return false;
    }

    @Override
    public SortedNumericDocValues longValues(LeafReaderContext ctx) {
        if (valuesSource instanceof GeoPoint) {
            return new GeoPointCellValues(((GeoPoint) valuesSource).geoPointValues(ctx), precision, encoder);
        } else if (valuesSource instanceof Bytes) {
            try {
                return new GeoShapeCellValues(valuesSource.bytesValues(ctx), precision, encoder);
            } catch (IOException e) {
                throw new IllegalArgumentException("uh oh too", e);
            }
        } else {
            throw new IllegalArgumentException("uh oh");
        }
    }

    @Override
    public SortedNumericDoubleValues doubleValues(LeafReaderContext ctx) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SortedBinaryDocValues bytesValues(LeafReaderContext ctx) {
        throw new UnsupportedOperationException();
    }

    /**
     * The encoder to use to convert a geopoint's (lon, lat, precision) into
     * a long-encoded bucket key for aggregating.
     */
    @FunctionalInterface
    public interface GeoPointLongEncoder {
        long encode(double lon, double lat, int precision);
    }

    private static class GeoPointCellValues extends AbstractSortingNumericDocValues {
        private MultiGeoPointValues geoValues;
        private int precision;
        private GeoPointLongEncoder encoder;

        protected GeoPointCellValues(MultiGeoPointValues geoValues, int precision, GeoPointLongEncoder encoder) {
            this.geoValues = geoValues;
            this.precision = precision;
            this.encoder = encoder;
        }

        @Override
        public boolean advanceExact(int docId) throws IOException {
            if (geoValues.advanceExact(docId)) {
                resize(geoValues.docValueCount());
                for (int i = 0; i < docValueCount(); ++i) {
                    org.elasticsearch.common.geo.GeoPoint target = geoValues.nextValue();
                    values[i] = encoder.encode(target.getLon(), target.getLat(), precision);
                }
                sort();
                return true;
            } else {
                return false;
            }
        }
    }

    private static class GeoShapeCellValues extends AbstractSortingNumericDocValues {
        private SortedBinaryDocValues binaryPolygon2DValues;
        private int precision;
        private GeoPointLongEncoder encoder;

        protected GeoShapeCellValues(SortedBinaryDocValues geoValues, int precision, GeoPointLongEncoder encoder) {
            this.binaryPolygon2DValues = geoValues;
            this.precision = precision;
            this.encoder = encoder;
        }

        @Override
        public boolean advanceExact(int docId) throws IOException {
//            function tileToLatitude(y, tileCount) {
//  const radians = Math.atan(sinh(Math.PI - (2 * Math.PI * y / tileCount)));
//  const lat = 180 / Math.PI * radians;
//                return _.round(lat, DECIMAL_DEGREES_PRECISION);
//            }
//
//
//            function tileToLongitude(x, tileCount) {
//  const lon = (x / tileCount * 360) - 180;
//                return _.round(lon, DECIMAL_DEGREES_PRECISION);
//            }
            if (binaryPolygon2DValues.advanceExact(docId)) {
                resize(binaryPolygon2DValues.docValueCount());
                int idx = 0;
                for (int i = 0; i < 1; ++i) {
                    Polygon2D target = Polygon2DUtils.bytesToPolygon2D(binaryPolygon2DValues.nextValue());
                    Point topLeft = new Point(target.maxLat, target.minLon);
                    Point bottomRight = new Point(target.minLat, target.maxLon);
                    // { zoom, x, y }
                    int[] topLeftTile = GeoTileUtils.parseHash(GeoTileUtils.longEncode(topLeft.getLon(), topLeft.getLat(), precision));
                    int[] bottomRightTile = GeoTileUtils.parseHash(GeoTileUtils.longEncode(bottomRight.getLon(), bottomRight.getLat(), precision));
                    int minX = topLeftTile[1];
                    int minY = topLeftTile[2];
                    int maxX = bottomRightTile[1];
                    int maxY = bottomRightTile[2];
                    int si = idx + (1 + maxX - minX) * (1 + maxY - minY);
                    resize(idx + si);
                    for (int x = minX; x <= maxX; x++) {
                        for (int y = minY; y <= maxY; y++) {
                            long hash = ((long) precision << ZOOM_SHIFT) | (x << MAX_ZOOM) | y;
                            System.out.println(GeoTileUtils.stringEncode(hash));
                            values[idx++] = hash;
                        }
                    }
                    // this is the bounding box, can get more specific
                }
                sort();
                return true;
            } else {
                return false;
            }
        }
    }
}
