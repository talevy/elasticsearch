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
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.index.fielddata.AbstractSortingNumericDocValues;
import org.elasticsearch.index.fielddata.MultiGeoValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;

/**
 * Wrapper class to help convert {@link MultiGeoValues}
 * to numeric long values for bucketing.
 */
public class CellIdSource extends ValuesSource.Numeric {
    private final ValuesSource.Geo valuesSource;
    private final int precision;
    private final GeoGridTiler encoder;
    private final GeoBoundingBox geoBoundingBox;

    public CellIdSource(Geo valuesSource, int precision, GeoBoundingBox geoBoundingBox, GeoGridTiler encoder) {
        this.valuesSource = valuesSource;
        //different GeoPoints could map to the same or different hashing cells.
        this.precision = precision;
        this.geoBoundingBox = geoBoundingBox;
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
        MultiGeoValues geoValues = valuesSource.geoValues(ctx);
        if (precision == 0) {
            // special case, precision 0 is the whole world
            return new AllCellValues(geoValues, encoder);
        }
        ValuesSourceType vs = geoValues.valuesSourceType();
        if (CoreValuesSourceType.GEOPOINT == vs) {
            // docValues are geo points
            return new GeoPointCellValues(geoValues, precision, geoBoundingBox, encoder);
        } else if (CoreValuesSourceType.GEOSHAPE == vs || CoreValuesSourceType.GEO == vs) {
            // docValues are geo shapes
            return new GeoShapeCellValues(geoValues, precision, geoBoundingBox, encoder);
        } else {
            throw new IllegalArgumentException("unsupported geo type");
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

    /** Sorted numeric doc values for geo shapes */
    protected static class GeoShapeCellValues extends AbstractSortingNumericDocValues {
        private MultiGeoValues geoValues;
        private int precision;
        private GeoBoundingBox geoBoundingBox;
        private GeoGridTiler tiler;

        protected GeoShapeCellValues(MultiGeoValues geoValues, int precision, GeoBoundingBox geoBoundingBox, GeoGridTiler tiler) {
            this.geoValues = geoValues;
            this.precision = precision;
            this.geoBoundingBox = geoBoundingBox;
            this.tiler = tiler;
        }

        protected void resizeCell(int newSize) {
            resize(newSize);
        }

        protected void add(int idx, long value) {
           values[idx] = value;
        }

        // for testing
        protected long[] getValues() {
            return values;
        }

        @Override
        public boolean advanceExact(int docId) throws IOException {
            if (geoValues.advanceExact(docId)) {
                ValuesSourceType vs = geoValues.valuesSourceType();
                MultiGeoValues.GeoValue target = geoValues.nextValue();
                // TODO(talevy): determine reasonable circuit-breaker here
                resize(0);
                tiler.setValues(this, target, precision, geoBoundingBox);
                sort();
                return true;
            } else {
                return false;
            }
        }
    }

    /** Sorted numeric doc values for geo points */
    protected static class GeoPointCellValues extends AbstractSortingNumericDocValues {
        private MultiGeoValues geoValues;
        private int precision;
        private GeoBoundingBox geoBoundingBox;
        private GeoGridTiler tiler;

        protected GeoPointCellValues(MultiGeoValues geoValues, int precision, GeoBoundingBox geoBoundingBox, GeoGridTiler tiler) {
            this.geoValues = geoValues;
            this.precision = precision;
            this.geoBoundingBox = geoBoundingBox;
            this.tiler = tiler;
        }

        // for testing
        protected long[] getValues() {
            return values;
        }

        @Override
        public boolean advanceExact(int docId) throws IOException {
            if (geoValues.advanceExact(docId)) {
                int docValueCount = geoValues.docValueCount();
                resize(docValueCount);
                int j = 0;
                for (int i = 0; i < docValueCount; i++) {
                    MultiGeoValues.GeoValue target = geoValues.nextValue();
                    if (geoBoundingBox.isUnbounded() || geoBoundingBox.pointInBounds(target.lon(), target.lat())) {
                        values[j++] = tiler.encode(target.lon(), target.lat(), precision);
                    }
                }
                resize(j);
                sort();
                return true;
            } else {
                return false;
            }
        }
    }

    /** Sorted numeric doc values for precision 0 */
    protected static class AllCellValues extends AbstractSortingNumericDocValues {
        private MultiGeoValues geoValues;

        protected AllCellValues(MultiGeoValues geoValues, GeoGridTiler tiler) {
            this.geoValues = geoValues;
            resize(1);
            values[0] = tiler.encode(0, 0, 0);
        }

        // for testing
        protected long[] getValues() {
            return values;
        }

        @Override
        public boolean advanceExact(int docId) throws IOException {
            resize(1);
            return geoValues.advanceExact(docId);
        }
    }
}
