package org.elasticsearch.search.aggregations.bucket.geogrid;

import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

public class ParsedGeoHashGridBucket extends ParsedGeoGridBucket {

    @Override
    public GeoPoint getKey() {
        return GeoPoint.fromGeohash(geohashAsString);
    }

    @Override
    public String getKeyAsString() {
        return geohashAsString;
    }

    static ParsedGeoHashGridBucket fromXContent(XContentParser parser) throws IOException {
        return parseXContent(parser, false, ParsedGeoHashGridBucket::new, (p, bucket) -> bucket.geohashAsString = p.textOrNull());
    }
}
