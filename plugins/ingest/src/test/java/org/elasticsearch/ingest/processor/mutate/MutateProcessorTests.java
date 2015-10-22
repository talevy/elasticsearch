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

package org.elasticsearch.ingest.processor.mutate;

import org.elasticsearch.ingest.Data;
import org.elasticsearch.ingest.processor.Processor;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.Arrays;
import java.util.HashMap;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;


public class MutateProcessorTests extends ESTestCase {
    private Data data;
    private MutateProcessor.Builder builder;

    @Before
    public void setData() {
        data = new Data("index", "type", "id",
                new HashMap<String, Object>() {{
                    put("foo", "bar");
                    put("alpha", "aBcD");
                    put("num", "64");
                    put("to_strip", " clean    ");
                    put("arr", Arrays.asList("1", "2", "3"));
                    put("ip", "127.0.0.1");
                    put("fizz", new HashMap<String, Object>() {{
                        put("buzz", "hello world");
                    }});
                }});
        builder = new MutateProcessor.Builder();
    }

    public void testUpdate() {
        builder.setUpdate(new HashMap<String, Object>() {{
            put("foo", 123);
        }});

        Processor processor = builder.build();
        processor.execute(data);
        assertThat(data.getProperty("foo"), equalTo(123));
    }

    public void testRename() {
        builder.setRename(new HashMap<String, String>() {{
            put("foo", "bar");
        }});

        Processor processor = builder.build();
        processor.execute(data);
        assertThat(data.getProperty("bar"), equalTo("bar"));
    }

    public void testConvert() {
        builder.setConvert(new HashMap<String, String>() {{
            put("num", "integer");
        }});

        Processor processor = builder.build();
        processor.execute(data);
        assertThat(data.getProperty("num"), equalTo(64));
    }

    public void testSplit() {
        builder.setSplit(new HashMap<String, String>() {{
            put("ip", "\\.");
        }});

        Processor processor = builder.build();
        processor.execute(data);
        assertThat(data.getProperty("ip"), equalTo(Arrays.asList("127", "0", "0", "1")));
    }

    public void testGsub() {
        // TODO(talevy)
    }

    public void testJoin() {
        builder.setJoin(new HashMap<String, String>() {{
            put("arr", "-");
        }});

        Processor processor = builder.build();
        processor.execute(data);
        assertThat(data.getProperty("arr"), equalTo("1-2-3"));
    }

    public void testMerge() {
        // TODO(talevy)
    }

    public void testRemove() {
        builder.setRemove(Arrays.asList("foo", "ip"));

        Processor processor = builder.build();
        processor.execute(data);
        assertThat(data.getProperty("foo"), nullValue());
        assertThat(data.getProperty("ip"), nullValue());
    }

    public void testStrip() {
        builder.setStrip(Arrays.asList("to_strip", "foo"));

        Processor processor = builder.build();
        processor.execute(data);
        assertThat(data.getProperty("foo"), equalTo("bar"));
        assertThat(data.getProperty("to_strip"), equalTo("clean"));
    }

    public void testUppercase() {
        builder.setUppercase(Arrays.asList("foo"));
        Processor processor = builder.build();
        processor.execute(data);
        assertThat(data.getProperty("foo"), equalTo("BAR"));
    }

    public void testLowercase() {
        builder.setLowercase(Arrays.asList("alpha"));
        Processor processor = builder.build();
        processor.execute(data);
        assertThat(data.getProperty("alpha"), equalTo("abcd"));
    }
}
