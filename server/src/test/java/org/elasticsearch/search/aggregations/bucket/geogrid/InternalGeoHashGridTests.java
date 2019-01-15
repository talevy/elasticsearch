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

import org.apache.lucene.index.IndexWriter;
import org.elasticsearch.common.geo.GeoHashUtils;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.test.InternalMultiBucketAggregationTestCase;
import org.elasticsearch.search.aggregations.ParsedMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashGrid.Bucket;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InternalGeoHashGridTests extends InternalMultiBucketAggregationTestCase<GeoHashGrid> {

    @Override
    protected int minNumberOfBuckets() {
        return 1;
    }

    @Override
    protected int maxNumberOfBuckets() {
        return 3;
    }

    @Override
    protected GeoHashGrid createTestInstance(String name,
                                             List<PipelineAggregator> pipelineAggregators,
                                             Map<String, Object> metaData,
                                             InternalAggregations aggregations) {
        int size = randomNumberOfBuckets();
        List<GeoHashGrid.Bucket> buckets = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            double latitude = randomDoubleBetween(-90.0, 90.0, false);
            double longitude = randomDoubleBetween(-180.0, 180.0, false);

            long geoHashAsLong = GeoHashUtils.longEncode(longitude, latitude, 4);
            buckets.add(new GeoHashGrid.Bucket(geoHashAsLong, randomInt(IndexWriter.MAX_DOCS), aggregations));
        }
        return new GeoHashGrid(name, size, buckets, pipelineAggregators, metaData);
    }

    @Override
    protected Writeable.Reader<GeoHashGrid> instanceReader() {
        return GeoHashGrid::new;
    }

    @Override
    protected void assertReduced(GeoHashGrid reduced, List<GeoHashGrid> inputs) {
        Map<Long, List<GeoHashGrid.Bucket>> map = new HashMap<>();
        for (GeoHashGrid input : inputs) {
            for (GeoGrid.Bucket bucket : input.getBuckets()) {
                GeoHashGrid.Bucket internalBucket = (GeoHashGrid.Bucket) bucket;
                List<GeoHashGrid.Bucket> buckets = map.get(internalBucket.geohashAsLong);
                if (buckets == null) {
                    map.put(internalBucket.geohashAsLong, buckets = new ArrayList<>());
                }
                buckets.add(internalBucket);
            }
        }
        List<GeoHashGrid.Bucket> expectedBuckets = new ArrayList<>();
        for (Map.Entry<Long, List<GeoHashGrid.Bucket>> entry : map.entrySet()) {
            long docCount = 0;
            for (GeoHashGrid.Bucket bucket : entry.getValue()) {
                docCount += bucket.docCount;
            }
            expectedBuckets.add(new GeoHashGrid.Bucket(entry.getKey(), docCount, InternalAggregations.EMPTY));
        }
        expectedBuckets.sort((first, second) -> {
            int cmp = Long.compare(second.docCount, first.docCount);
            if (cmp == 0) {
                return second.compareTo(first);
            }
            return cmp;
        });
        int requestedSize = inputs.get(0).getRequiredSize();
        expectedBuckets = expectedBuckets.subList(0, Math.min(requestedSize, expectedBuckets.size()));
        assertEquals(expectedBuckets.size(), reduced.getBuckets().size());
        for (int i = 0; i < reduced.getBuckets().size(); i++) {
            GeoGrid.Bucket expected = expectedBuckets.get(i);
            GeoGrid.Bucket actual = reduced.getBuckets().get(i);
            assertEquals(expected.getDocCount(), actual.getDocCount());
            assertEquals(expected.getKey(), actual.getKey());
        }
    }

    @Override
    protected Class<? extends ParsedMultiBucketAggregation> implementationClass() {
        return ParsedGeoGrid.class;
    }

    @Override
    protected GeoHashGrid mutateInstance(GeoHashGrid instance) {
        String name = instance.getName();
        int size = instance.getRequiredSize();
        List<Bucket> buckets = instance.getBuckets();
        List<PipelineAggregator> pipelineAggregators = instance.pipelineAggregators();
        Map<String, Object> metaData = instance.getMetaData();
        switch (between(0, 3)) {
        case 0:
            name += randomAlphaOfLength(5);
            break;
        case 1:
            buckets = new ArrayList<>(buckets);
            buckets.add(
                    new GeoHashGrid.Bucket(randomNonNegativeLong(), randomInt(IndexWriter.MAX_DOCS), InternalAggregations.EMPTY));
            break;
        case 2:
            size = size + between(1, 10);
            break;
        case 3:
            if (metaData == null) {
                metaData = new HashMap<>(1);
            } else {
                metaData = new HashMap<>(instance.getMetaData());
            }
            metaData.put(randomAlphaOfLength(15), randomInt());
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }
        return new GeoHashGrid(name, size, buckets, pipelineAggregators, metaData);
    }

}
