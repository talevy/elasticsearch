package org.elasticsearch.search.aggregations.bucket.geogrid;

import org.elasticsearch.common.geo.GeoHashUtils;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.aggregations.InternalAggregations;

import java.io.IOException;

class GeoHashGridBucket extends InternalGeoGridBucket<GeoHashGridBucket> {
    GeoHashGridBucket(long geohashAsLong, long docCount, InternalAggregations aggregations) {
        super(geohashAsLong, docCount, aggregations);
    }

    /**
     * Read from a stream.
     */
    public GeoHashGridBucket(StreamInput in) throws IOException {
        super(in);
    }


    @Override
    GeoHashGridBucket buildBucket(InternalGeoGridBucket bucket, long geoHashAsLong, long docCount, InternalAggregations aggregations) {
        return new GeoHashGridBucket(geoHashAsLong, docCount, aggregations);
    }

    @Override
    public String getKeyAsString() {
        return GeoHashUtils.stringEncode(geohashAsLong);
    }

    @Override
    public GeoPoint getKey() {
        return GeoPoint.fromGeohash(geohashAsLong);
    }
}
