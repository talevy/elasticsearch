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

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.plugin.wordcount.WordCountFieldMapper;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceParserHelper;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;

import static org.elasticsearch.plugin.wordcount.WordCountPlugin.WORDCOUNT_FIELD;


public class WordCountSumAggregationBuilder extends SumAggregationBuilder {
    public static final String NAME = "wordcount_sum";

    private static final ObjectParser<WordCountSumAggregationBuilder, Void> PARSER;

    static {
        PARSER = new ObjectParser<>(WordCountSumAggregationBuilder.NAME);
        ValuesSourceParserHelper.declareNumericFields(PARSER, true, true, false);
    }

    public static AggregationBuilder parse(String aggregationName, XContentParser parser) throws IOException {
        return PARSER.parse(parser, new WordCountSumAggregationBuilder(aggregationName), null);
    }

    public WordCountSumAggregationBuilder(String name) {
        super(name);
    }

    public WordCountSumAggregationBuilder(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    protected ValuesSourceConfig<ValuesSource.Numeric> resolveConfig(SearchContext context) {
        // resolve multifield
        MappedFieldType mapper = context.getQueryShardContext().fieldMapper(field());
        String field = field();
        if (mapper != null) {
            String wcField = mapper.name() + "." + WORDCOUNT_FIELD;
            mapper = context.getQueryShardContext().fieldMapper(wcField);
            if (mapper != null) {
                field = wcField;
            }
        }
        return ValuesSourceConfig.resolve(context.getQueryShardContext(),
            valueType(), field, script(), missing(), timeZone(), format());
    }
}
