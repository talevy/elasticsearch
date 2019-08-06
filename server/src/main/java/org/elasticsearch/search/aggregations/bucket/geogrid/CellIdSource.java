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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.index.fielddata.AbstractSortingNumericDocValues;
import org.elasticsearch.index.fielddata.MultiGeoValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.aggregations.support.ValuesSource;

import java.io.IOException;

import static org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils.MAX_ZOOM;
import static org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils.ZOOM_SHIFT;

/**
 * Wrapper class to help convert {@link MultiGeoValues}
 * to numeric long values for bucketing.
 */
class CellIdSource extends ValuesSource.Numeric {
    private final Geo valuesSource;
    private final int precision;
    private final GeoPointLongEncoder encoder;

    CellIdSource(Geo valuesSource, int precision, GeoPointLongEncoder encoder) {
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
        return new CellValues(valuesSource.geoValues(ctx), precision, encoder);
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

    private static class CellValues extends AbstractSortingNumericDocValues {
        private MultiGeoValues geoValues;
        private int precision;
        private GeoPointLongEncoder encoder;

        protected CellValues(MultiGeoValues geoValues, int precision, GeoPointLongEncoder encoder) {
            this.geoValues = geoValues;
            this.precision = precision;
            this.encoder = encoder;
        }

        @Override
        public boolean advanceExact(int docId) throws IOException {
            if (geoValues.advanceExact(docId)) {
                resize(geoValues.docValueCount());
                for (int i = 0; i < docValueCount(); ++i) {
                    // TODO(talevy): make more generic for tile/hash
                    MultiGeoValues.GeoValue target = geoValues.nextValue();
                    MultiGeoValues.BoundingBox box = target.boundingBox();
                    org.elasticsearch.common.geo.GeoPoint topLeft = box.topLeft();
                    org.elasticsearch.common.geo.GeoPoint bottomRight = box.bottomRight();
                    int[] topLeftTile = GeoTileUtils.parseHash(GeoTileUtils.longEncode(topLeft.getLon(), topLeft.getLat(), precision));
                    int[] bottomRightTile = GeoTileUtils.parseHash(GeoTileUtils.longEncode(bottomRight.getLon(), bottomRight.getLat(), precision));
                    int minX = topLeftTile[1];
                    int minY = topLeftTile[2];
                    int maxX = bottomRightTile[1];
                    int maxY = bottomRightTile[2];
                    int candidateBucketCount = (1 + maxX - minX) * (1 + maxY - minY);
                    // resize values to contain all matches for a shape
                    int size = docValueCount();
                    resize(size + candidateBucketCount);

                    int actualBucketsMatched = 0;
                    for (int x = minX; x <= maxX; x++) {
                        for (int y = minY; y <= maxY; y++) {
                            long hash = ((long) precision << ZOOM_SHIFT) | (x << MAX_ZOOM) | y;
                            GeoTileUtils.hashToGeoPoint(hash);
                            if (target.intersects())
                            values[i + actualBucketsMatched] = hash;
                            actualBucketsMatched++;
                        }
                    }

                    // resize down to actual size
                    resize(size + (idx - i));
                    //values[i] = encoder.encode(target.lon(), target.lat(), precision);
                }
                sort();
                return true;
            } else {
                return false;
            }
        }
    }
}
