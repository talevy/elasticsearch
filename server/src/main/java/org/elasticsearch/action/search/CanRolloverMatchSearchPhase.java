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
package org.elasticsearch.action.search;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.search.SearchService.CanMatchResponse;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.MinAndMax;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.Transport;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

// TODO(talevy)
final class CanRolloverMatchSearchPhase extends AbstractSearchAsyncAction<CanMatchResponse> {

    private final Function<GroupShardsIterator<SearchShardIterator>, SearchPhase> phaseFactory;
    private final GroupShardsIterator<SearchShardIterator> shardsIts;

    CanRolloverMatchSearchPhase(Logger logger, SearchTransportService searchTransportService,
                                BiFunction<String, String, Transport.Connection> nodeIdToConnection,
                                Map<String, AliasFilter> aliasFilter, Map<String, Float> concreteIndexBoosts,
                                Map<String, Set<String>> indexRoutings,
                                Executor executor, SearchRequest request,
                                ActionListener<SearchResponse> listener, GroupShardsIterator<SearchShardIterator> shardsIts,
                                TransportSearchAction.SearchTimeProvider timeProvider, ClusterState clusterState,
                                SearchTask task, Function<GroupShardsIterator<SearchShardIterator>, SearchPhase> phaseFactory,
                                SearchResponse.Clusters clusters) {
        //We set max concurrent shard requests to the number of shards so no throttling happens for can_match requests
        super("can_match_rollover", logger, searchTransportService, nodeIdToConnection, aliasFilter, concreteIndexBoosts, indexRoutings,
                executor, request, listener, shardsIts, timeProvider, clusterState, task,
                new CanMatchSearchPhaseResults(shardsIts.size()), shardsIts.size(), clusters);
        this.phaseFactory = phaseFactory;
        this.shardsIts = shardsIts;
    }

    @Override
    protected void executePhaseOnShard(SearchShardIterator shardIt, ShardRouting shard,
                                       SearchActionListener<CanMatchResponse> listener) {
        getSearchTransport().sendCanMatchRollover(getConnection(shardIt.getClusterAlias(), shard.currentNodeId()),
            buildShardSearchRequest(shardIt), getTask(), listener);
    }

    @Override
    protected SearchPhase getNextPhase(SearchPhaseResults<CanMatchResponse> results,
                                       SearchPhaseContext context) {

        CanMatchSearchPhaseResults phaseResults = (CanMatchSearchPhaseResults) results;
        int cardinality = phaseResults.getNumPossibleMatches();
        FixedBitSet possibleMatches = phaseResults.getPossibleMatches();
        if (cardinality == 0) {
            // this is a special case where we have no hit but we need to get at least one search response in order
            // to produce a valid search result with all the aggs etc.
            possibleMatches.set(0);
        }
        int i = 0;
        for (SearchShardIterator iter : shardsIts) {
            if (possibleMatches.get(i++)) {
                iter.reset();
            } else {
                iter.resetAndSkip();
            }
        }
        return phaseFactory.apply(shardsIts);
    }

    private static final class CanMatchSearchPhaseResults extends SearchPhaseResults<CanMatchResponse> {
        private final FixedBitSet possibleMatches;
        private final MinAndMax<?>[] minAndMaxes;
        private int numPossibleMatches;

        CanMatchSearchPhaseResults(int size) {
            super(size);
            possibleMatches = new FixedBitSet(size);
            minAndMaxes = new MinAndMax[size];
        }

        @Override
        void consumeResult(CanMatchResponse result, Runnable next) {
            try {
                consumeResult(result.getShardIndex(), result.canMatch(), result.estimatedMinAndMax());
            } finally {
                next.run();
            }
        }

        @Override
        boolean hasResult(int shardIndex) {
            return false; // unneeded
        }

        @Override
        void consumeShardFailure(int shardIndex) {
            // we have to carry over shard failures in order to account for them in the response.
            consumeResult(shardIndex, true, null);
        }

        synchronized void consumeResult(int shardIndex, boolean canMatch, MinAndMax<?> minAndMax) {
            if (canMatch) {
                possibleMatches.set(shardIndex);
                numPossibleMatches++;
            }
            minAndMaxes[shardIndex] = minAndMax;
        }

        synchronized int getNumPossibleMatches() {
            return numPossibleMatches;
        }

        synchronized FixedBitSet getPossibleMatches() {
            return possibleMatches;
        }

        @Override
        Stream<CanMatchResponse> getSuccessfulResults() {
            return Stream.empty();
        }
    }
}
