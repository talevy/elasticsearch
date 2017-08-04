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

package org.elasticsearch.ingest.common;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This class provides a mechanism to detect which Grok patterns
 * match a specific input string
 */
final class GrokDiscover {

    private final Map<String, String> patternBank;

    GrokDiscover(Map<String, String> patternBank) {
        this.patternBank = patternBank;
    }

    List<GrokDiscoveryCandidate> discover(String input) {
        ArrayList<GrokDiscoveryCandidate> candidates = new ArrayList<>();
        for (Map.Entry<String, String> pattern : patternBank.entrySet()) {
            Grok grok = new Grok(patternBank, pattern.getValue());
            if (grok.match(input)) {
                candidates.add(
                    new GrokDiscoveryCandidate(pattern.getKey(), pattern.getValue(), grok.getExpression()));
            }
        }

        return candidates;
    }

    static class GrokDiscoveryCandidate implements Streamable, ToXContent {
        private String expressionName;
        private String expressionDefinition;
        private String expressionAsRegex;

        GrokDiscoveryCandidate() {
        }

        GrokDiscoveryCandidate(String expressionName, String expressionDefinition, String expressionAsRegex) {
            this.expressionName = expressionName;
            this.expressionDefinition = expressionDefinition;
            this.expressionAsRegex = expressionAsRegex;
        }

        public String getExpressionName() {
            return expressionName;
        }

        public String getExpressionDefinition() {
            return expressionDefinition;
        }

        public String getExpressionAsRegex() {
            return expressionAsRegex;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            this.expressionName = in.readString();
            this.expressionDefinition = in.readString();
            this.expressionAsRegex = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(expressionName);
            out.writeString(expressionDefinition);
            out.writeString(expressionAsRegex);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(); {
                builder.field("expression", expressionName);
                builder.field("definition", expressionDefinition);
                builder.field("definition_as_regex", expressionAsRegex);
            } builder.endObject();
            return builder;
        }
    }
}

