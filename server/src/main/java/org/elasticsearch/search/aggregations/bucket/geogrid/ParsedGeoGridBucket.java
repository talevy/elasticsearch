package org.elasticsearch.search.aggregations.bucket.geogrid;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.ParsedMultiBucketAggregation;

import java.io.IOException;

public abstract class ParsedGeoGridBucket extends ParsedMultiBucketAggregation.ParsedBucket implements GeoGrid.Bucket {

    protected String geohashAsString;

    @Override
    protected XContentBuilder keyToXContent(XContentBuilder builder) throws IOException {
        return builder.field(Aggregation.CommonFields.KEY.getPreferredName(), geohashAsString);
    }
}
