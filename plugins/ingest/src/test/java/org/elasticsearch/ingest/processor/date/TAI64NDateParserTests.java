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

package org.elasticsearch.ingest.processor.date;

import org.elasticsearch.test.ESTestCase;
import org.joda.time.DateTimeZone;

import static org.hamcrest.core.IsEqual.equalTo;

public class TAI64NDateParserTests extends ESTestCase {

    public void testParse() {
        TAI64NDateParser parser = new TAI64NDateParser(DateTimeZone.forOffsetHours(2));
        String input = "4000000050d506482dbdf024";
        String expected = "2012-12-22T03:00:46.767+02:00";
        assertThat(parser.parseDateTime("@" + input).toString(), equalTo(expected));
        assertThat(parser.parseDateTime(input).toString(), equalTo(expected));
    }
}
