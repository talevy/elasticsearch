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
package org.elasticsearch.plugin.wordcount;

import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.metrics.InternalSum;
import org.elasticsearch.search.aggregations.metrics.WordCountSumAggregationBuilder;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Arrays;
import java.util.Collection;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

public class WordCountIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(WordCountPlugin.class);
    }

    public void test() throws Exception {
        assertAcked(prepareCreate("test").addMapping("_doc", "body", "type=text"));
        final String source = "{\"body\":\"hello world\"}";
        indexRandom(true,
            client().prepareIndex("test", "_doc", "1").setSource(source, XContentType.JSON),
            client().prepareIndex("test", "_doc", "2").setSource(source, XContentType.JSON));
        GetResponse getResponse = client().prepareGet("test", "_doc", "1").setStoredFields("body.wordcount").get();
        assertNotNull(getResponse.getField("body.wordcount"));
        assertEquals(2, (int) getResponse.getField("body.wordcount").getValue());

        SearchResponse searchResponse = client().prepareSearch("test")
            .addAggregation(new WordCountSumAggregationBuilder("word_count").field("body")).get();
        InternalSum agg = searchResponse.getAggregations().get("word_count");
        assertThat(agg.value(), equalTo(4.0));

        GetMappingsResponse e = client().admin().indices().prepareGetMappings("test").get();
        e.getMappings();
    }
}
