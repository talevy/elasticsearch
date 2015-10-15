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

package org.elasticsearch.ingest.processor.grok;

import org.elasticsearch.ingest.Data;
import org.elasticsearch.ingest.processor.ConfigException;
import org.elasticsearch.ingest.processor.Processor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public final class GrokProcessor implements Processor {

    public static final String TYPE = "grok";
    public static final String PATTERNS_PATH = "/org/elasticsearch/ingest/processor/grokpatterns";

    private final String matchField;
    private final String matchPattern;
    private Grok grok;

    public GrokProcessor(String matchField, String matchPattern) throws ConfigException {
        this.matchField = matchField;
        this.matchPattern = matchPattern;

        this.grok = new Grok();

        // TODO(talevy): finalize a nicer way to manage these pattern files (properties file or filepaths)
        try {
            InputStream is = getClass().getResourceAsStream(PATTERNS_PATH);
            BufferedReader br = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
            String fileNameInLine;
            while ((fileNameInLine = br.readLine()) != null) {
                this.grok.loadFromStream(getClass().getResourceAsStream(PATTERNS_PATH + "/" + fileNameInLine));
            }
        } catch (IOException e) {
            throw new ConfigException(e);
        }
        this.grok.compile(this.matchPattern);
    }

    @Override
    public void execute(Data data) {
        Object field = data.getProperty(matchField);
        // TODO(talevy): handle invalid field types
        if (field instanceof String) {
            Map<String, Object> matches = grok.captures((String) field);
            if (matches != null) {
                matches.forEach((k, v) -> data.addField(k, v));
            }
        }
    }

    public static class Builder implements Processor.Builder {

        private String matchField;
        private String matchPattern;

        public void setMatchField(String matchField) {
            this.matchField = matchField;
        }

        public void setMatchPattern(String matchPattern) {
            this.matchPattern = matchPattern;
        }

        public void fromMap(Map<String, Object> config) {
            this.matchField = (String) config.get("field");
            this.matchPattern = (String) config.get("pattern");
        }

        @Override
        public Processor build() throws ConfigException {
            return new GrokProcessor(matchField, matchPattern);
        }

        public static class Factory implements Processor.Builder.Factory {

            @Override
            public Processor.Builder create() {
                return new Builder();
            }
        }

    }
}
