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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.metrics.InternalSum;
import org.elasticsearch.search.aggregations.metrics.WordCountSumAggregationBuilder;

public class WordCountPlugin extends Plugin implements MapperPlugin, SearchPlugin {

    public static final String WORDCOUNT_FIELD = "wordcount";

    public Map<String, Mapper.TypeParser> getMappers() {
        return Collections.singletonMap(WordCountFieldMapper.CONTENT_TYPE, new WordCountFieldMapper.TypeParser());
    }

    public Function<String, Predicate<String>> getFieldFilter() {
        return index -> field -> !field.contains(WORDCOUNT_FIELD);
    }

    @SuppressWarnings("rawtypes")
    public Map<String, List<Supplier<Mapper.Builder>>> getMultifieldMappers() {
        return Collections.singletonMap(TextFieldMapper.CONTENT_TYPE,
            Collections.singletonList(() -> new WordCountFieldMapper.Builder(WORDCOUNT_FIELD)));
    }

    public List<AggregationSpec> getAggregations() {
        AggregationSpec spec = new AggregationSpec(WordCountSumAggregationBuilder.NAME,
            WordCountSumAggregationBuilder::new, WordCountSumAggregationBuilder::parse)
            .addResultReader(InternalSum::new);
        return Collections.singletonList(spec);
    }
}
