/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.rollup.v2;

import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.rollup.action.RollupV2Action;
import org.elasticsearch.xpack.core.rollup.job.DateHistogramGroupConfig;
import org.elasticsearch.xpack.core.rollup.job.GroupConfig;
import org.elasticsearch.xpack.core.rollup.job.MetricConfig;
import org.elasticsearch.xpack.core.rollup.job.RollupJobConfig;
import org.elasticsearch.xpack.core.rollup.job.RollupV2Job;
import org.elasticsearch.xpack.core.rollup.job.TermsGroupConfig;
import org.elasticsearch.xpack.rollup.Rollup;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class RollupV2IT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(Rollup.class, LocalStateCompositeXPackPlugin.class);
    }

    public void testRollup() throws Exception {
        String sourceIndex = "test";
        String rollupIndex = "rollup_test";

        client().admin().indices().prepareCreate(sourceIndex)
            .setMapping("date", "type=date",
                "numeric_1", "type=double",
                "numeric_2", "type=float",
                "categorical_1", "type=keyword")
            .get();

        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        bulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

        for (int i = 0; i < 5; i++) {
            IndexRequest indexRequest = new IndexRequest(sourceIndex);

            // We insert one odd value out of 5 for one feature
            String docId = "_doc_" + i;
            indexRequest.id(docId);
            indexRequest.source("date", 0.0, "numeric_1", i == 0 ? 100.0 : 1.0, "numeric_2", 1.0, "categorical_1", "foo_" + i);
            bulkRequestBuilder.add(indexRequest);
        }
        BulkResponse bulkResponse = bulkRequestBuilder.get();
        if (bulkResponse.hasFailures()) {
            fail("Failed to index data: " + bulkResponse.buildFailureMessage());
        }

        GroupConfig groupConfig = new GroupConfig(new DateHistogramGroupConfig.FixedInterval("date",
            new DateHistogramInterval("1d")), null, new TermsGroupConfig("categorical_1"));
        MetricConfig numeric1Metrics = new MetricConfig("numeric_1", List.of("max"));
        MetricConfig numeric2Metrics = new MetricConfig("numeric_2", List.of("max"));
        RollupJobConfig config = new RollupJobConfig("job_id", sourceIndex, rollupIndex, "* * * *", 1000,
            groupConfig, List.of(numeric1Metrics, numeric2Metrics), null);
        RollupV2Job job = new RollupV2Job(config, Collections.emptyMap());
        RollupV2Action.Request rollupRequest = new RollupV2Action.Request(sourceIndex, job);
        RollupV2Action.Response response = client().execute(RollupV2Action.INSTANCE, rollupRequest).get();
        assertTrue(response.isStarted());

        assertBusy(() -> {
            ListTasksResponse tasksResponse = client().admin().cluster().prepareListTasks().get();
            boolean rollupRunning = false;
            for (TaskInfo taskInfo : tasksResponse.getTasks()) {
                if (taskInfo.getAction().startsWith("xpack/rollupV2/job")) {
                    rollupRunning = true;
                }
            }
            try {
                SearchResponse resp = client().prepareSearch(rollupIndex).get();
                assertThat(resp.getHits().getTotalHits().value, equalTo(5L));
                assertFalse(rollupRunning);
            } catch (Exception e) {
                fail();
            }
        });

        SearchResponse rolledUpSearch = client().prepareSearch(rollupIndex, sourceIndex)
            .addAggregation(AggregationBuilders.dateHistogram("date_histo")
                .field("date").fixedInterval(new DateHistogramInterval("1m")))
            .get();
        assertThat(rolledUpSearch.getHits().getTotalHits().value, equalTo(5L));
    }
}
