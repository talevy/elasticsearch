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

package org.elasticsearch.action.support;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.action.support.IndicesOptions.Option;
import org.elasticsearch.action.support.IndicesOptions.WildcardStates;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.test.VersionUtils.randomVersionBetween;
import static org.hamcrest.CoreMatchers.equalTo;

public class IndicesOptionsTests extends ESTestCase {
    static IndicesOptions randomIndicesOptions() {
        return new IndicesOptions(EnumSet.copyOf(randomSubsetOf(Arrays.asList(Option.values()))),
            EnumSet.copyOf(randomSubsetOf(Arrays.asList(WildcardStates.values()))));
    }

    public void testSerialization() throws Exception {
        int iterations = randomIntBetween(5, 20);
        for (int i = 0; i < iterations; i++) {
            Version version = randomVersionBetween(random(), Version.V_7_0_0_alpha1, null);
            IndicesOptions indicesOptions = randomIndicesOptions();

            BytesStreamOutput output = new BytesStreamOutput();
            output.setVersion(version);
            indicesOptions.writeIndicesOptions(output);

            StreamInput streamInput = output.bytes().streamInput();
            streamInput.setVersion(version);
            IndicesOptions indicesOptions2 = IndicesOptions.readIndicesOptions(streamInput);

            assertThat(indicesOptions2.ignoreUnavailable(), equalTo(indicesOptions.ignoreUnavailable()));
            assertThat(indicesOptions2.allowNoIndices(), equalTo(indicesOptions.allowNoIndices()));
            assertThat(indicesOptions2.expandWildcardsOpen(), equalTo(indicesOptions.expandWildcardsOpen()));
            assertThat(indicesOptions2.expandWildcardsClosed(), equalTo(indicesOptions.expandWildcardsClosed()));

            assertThat(indicesOptions2.forbidClosedIndices(), equalTo(indicesOptions.forbidClosedIndices()));
            assertThat(indicesOptions2.allowAliasesToMultipleIndices(), equalTo(indicesOptions.allowAliasesToMultipleIndices()));

            assertEquals(indicesOptions2.ignoreAliases(), indicesOptions.ignoreAliases());
        }
    }

    public void testSerializationPre70() throws Exception {
        int iterations = randomIntBetween(5, 20);
        for (int i = 0; i < iterations; i++) {
            Version version = randomVersionBetween(random(), null, Version.V_6_4_0);
            IndicesOptions indicesOptions = randomIndicesOptions();

            BytesStreamOutput output = new BytesStreamOutput();
            output.setVersion(version);
            indicesOptions.writeIndicesOptions(output);

            StreamInput streamInput = output.bytes().streamInput();
            streamInput.setVersion(version);
            IndicesOptions indicesOptions2 = IndicesOptions.readIndicesOptions(streamInput);

            assertThat(indicesOptions2.ignoreUnavailable(), equalTo(indicesOptions.ignoreUnavailable()));
            assertThat(indicesOptions2.allowNoIndices(), equalTo(indicesOptions.allowNoIndices()));
            assertThat(indicesOptions2.expandWildcardsOpen(), equalTo(indicesOptions.expandWildcardsOpen()));
            assertThat(indicesOptions2.expandWildcardsClosed(), equalTo(indicesOptions.expandWildcardsClosed()));

            assertThat(indicesOptions2.forbidClosedIndices(), equalTo(indicesOptions.forbidClosedIndices()));
            assertThat(indicesOptions2.allowAliasesToMultipleIndices(), equalTo(indicesOptions.allowAliasesToMultipleIndices()));

            assertEquals(indicesOptions2.ignoreAliases(), indicesOptions.ignoreAliases());
        }
    }

    public void testFromOptions() {
        boolean ignoreUnavailable = randomBoolean();
        boolean allowNoIndices = randomBoolean();
        boolean expandToOpenIndices = randomBoolean();
        boolean expandToClosedIndices = randomBoolean();
        boolean allowAliasesToMultipleIndices = randomBoolean();
        boolean forbidClosedIndices = randomBoolean();
        boolean ignoreAliases = randomBoolean();

        IndicesOptions indicesOptions = randomIndicesOptions();

        assertThat(indicesOptions.ignoreUnavailable(), equalTo(ignoreUnavailable));
        assertThat(indicesOptions.allowNoIndices(), equalTo(allowNoIndices));
        assertThat(indicesOptions.expandWildcardsOpen(), equalTo(expandToOpenIndices));
        assertThat(indicesOptions.expandWildcardsClosed(), equalTo(expandToClosedIndices));
        assertThat(indicesOptions.allowAliasesToMultipleIndices(), equalTo(allowAliasesToMultipleIndices));
        assertThat(indicesOptions.allowAliasesToMultipleIndices(), equalTo(allowAliasesToMultipleIndices));
        assertThat(indicesOptions.forbidClosedIndices(), equalTo(forbidClosedIndices));
        assertEquals(ignoreAliases, indicesOptions.ignoreAliases());
    }

    public void testFromOptionsWithDefaultOptions() {
        boolean ignoreUnavailable = randomBoolean();
        boolean allowNoIndices = randomBoolean();
        boolean expandToOpenIndices = randomBoolean();
        boolean expandToClosedIndices = randomBoolean();

        IndicesOptions defaultOptions = randomIndicesOptions();

        IndicesOptions indicesOptions = IndicesOptions.fromOptions(ignoreUnavailable, allowNoIndices,expandToOpenIndices,
                expandToClosedIndices, defaultOptions);

        assertEquals(ignoreUnavailable, indicesOptions.ignoreUnavailable());
        assertEquals(allowNoIndices, indicesOptions.allowNoIndices());
        assertEquals(expandToOpenIndices, indicesOptions.expandWildcardsOpen());
        assertEquals(expandToClosedIndices, indicesOptions.expandWildcardsClosed());
        assertEquals(defaultOptions.allowAliasesToMultipleIndices(), indicesOptions.allowAliasesToMultipleIndices());
        assertEquals(defaultOptions.forbidClosedIndices(), indicesOptions.forbidClosedIndices());
        assertEquals(defaultOptions.ignoreAliases(), indicesOptions.ignoreAliases());
    }

    public void testFromParameters() {
        boolean expandWildcardsOpen = randomBoolean();
        boolean expandWildcardsClosed = randomBoolean();
        String expandWildcardsString;
        if (expandWildcardsOpen && expandWildcardsClosed) {
            if (randomBoolean()) {
                expandWildcardsString = "open,closed";
            } else {
                expandWildcardsString = "all";
            }
        } else if (expandWildcardsOpen) {
            expandWildcardsString = "open";
        } else if (expandWildcardsClosed) {
            expandWildcardsString = "closed";
        } else {
            expandWildcardsString = "none";
        }
        boolean ignoreUnavailable = randomBoolean();
        String ignoreUnavailableString = Boolean.toString(ignoreUnavailable);
        boolean allowNoIndices = randomBoolean();
        String allowNoIndicesString = Boolean.toString(allowNoIndices);

        IndicesOptions defaultOptions = randomIndicesOptions();

        IndicesOptions updatedOptions = IndicesOptions.fromParameters(expandWildcardsString, ignoreUnavailableString,
                allowNoIndicesString, defaultOptions);

        assertEquals(expandWildcardsOpen, updatedOptions.expandWildcardsOpen());
        assertEquals(expandWildcardsClosed, updatedOptions.expandWildcardsClosed());
        assertEquals(ignoreUnavailable, updatedOptions.ignoreUnavailable());
        assertEquals(allowNoIndices, updatedOptions.allowNoIndices());
        assertEquals(defaultOptions.allowAliasesToMultipleIndices(), updatedOptions.allowAliasesToMultipleIndices());
        assertEquals(defaultOptions.forbidClosedIndices(), updatedOptions.forbidClosedIndices());
        assertEquals(defaultOptions.ignoreAliases(), updatedOptions.ignoreAliases());
    }

    public void testSimpleByteBWC() {
        Map<Byte, IndicesOptions> old = new HashMap<>();
        // These correspond to each individual option (bit) in the old byte-based IndicesOptions
        old.put((byte) 0, new IndicesOptions(EnumSet.noneOf(Option.class), EnumSet.noneOf(WildcardStates.class)));
        old.put((byte) 1, new IndicesOptions(EnumSet.of(Option.IGNORE_UNAVAILABLE), EnumSet.noneOf(WildcardStates.class)));
        old.put((byte) 2, new IndicesOptions(EnumSet.of(Option.ALLOW_NO_INDICES), EnumSet.noneOf(WildcardStates.class)));
        old.put((byte) 4, new IndicesOptions(EnumSet.noneOf(Option.class), EnumSet.of(WildcardStates.OPEN)));
        old.put((byte) 8, new IndicesOptions(EnumSet.noneOf(Option.class), EnumSet.of(WildcardStates.CLOSED)));
        old.put((byte) 16, new IndicesOptions(EnumSet.of(Option.FORBID_ALIASES_TO_MULTIPLE_INDICES), EnumSet.noneOf(WildcardStates.class)));
        old.put((byte) 32, new IndicesOptions(EnumSet.of(Option.FORBID_CLOSED_INDICES), EnumSet.noneOf(WildcardStates.class)));
        old.put((byte) 64, new IndicesOptions(EnumSet.of(Option.IGNORE_ALIASES), EnumSet.noneOf(WildcardStates.class)));
        // Test a few multi-selected options
        old.put((byte) 13, new IndicesOptions(EnumSet.of(Option.IGNORE_UNAVAILABLE),
            EnumSet.of(WildcardStates.OPEN, WildcardStates.CLOSED)));
        old.put((byte) 19, new IndicesOptions(EnumSet.of(Option.IGNORE_UNAVAILABLE, Option.ALLOW_NO_INDICES,
            Option.FORBID_ALIASES_TO_MULTIPLE_INDICES), EnumSet.noneOf(WildcardStates.class)));
        old.put((byte) 24, new IndicesOptions(EnumSet.of(Option.FORBID_ALIASES_TO_MULTIPLE_INDICES), EnumSet.of(WildcardStates.CLOSED)));
        old.put((byte) 123, new IndicesOptions(EnumSet.of(Option.FORBID_ALIASES_TO_MULTIPLE_INDICES, Option.IGNORE_UNAVAILABLE,
            Option.ALLOW_NO_INDICES, Option.FORBID_CLOSED_INDICES, Option.IGNORE_ALIASES), EnumSet.of(WildcardStates.CLOSED)));

        for (Map.Entry<Byte, IndicesOptions> entry : old.entrySet()) {
            IndicesOptions indicesOptions2 = IndicesOptions.fromByte(entry.getKey());
            logger.info("--> 1 {}", entry.getValue().toString());
            logger.info("--> 2 {}", indicesOptions2.toString());
            assertThat("IndicesOptions for byte " + entry.getKey() + " differ for conversion",indicesOptions2, equalTo(entry.getValue()));
        }
    }

    public void testEqualityAndHashCode() {
        IndicesOptions indicesOptions = randomIndicesOptions();

        EqualsHashCodeTestUtils.checkEqualsAndHashCode(indicesOptions, opts -> {
            return IndicesOptions.fromOptions(opts.ignoreUnavailable(), opts.allowNoIndices(), opts.expandWildcardsOpen(),
                opts.expandWildcardsClosed(), opts.allowAliasesToMultipleIndices(), opts.forbidClosedIndices(), opts.ignoreAliases());
        }, opts -> {
            boolean mutated = false;
            boolean ignoreUnavailable = opts.ignoreUnavailable();
            boolean allowNoIndices = opts.allowNoIndices();
            boolean expandOpen = opts.expandWildcardsOpen();
            boolean expandClosed = opts.expandWildcardsClosed();
            boolean allowAliasesToMulti = opts.allowAliasesToMultipleIndices();
            boolean forbidClosed = opts.forbidClosedIndices();
            boolean ignoreAliases = opts.ignoreAliases();
            while (mutated == false) {
                if (randomBoolean()) {
                    ignoreUnavailable = !ignoreUnavailable;
                    mutated = true;
                }
                if (randomBoolean()) {
                    allowNoIndices = !allowNoIndices;
                    mutated = true;
                }
                if (randomBoolean()) {
                    expandOpen = !expandOpen;
                    mutated = true;
                }
                if (randomBoolean()) {
                    expandClosed = !expandClosed;
                    mutated = true;
                }
                if (randomBoolean()) {
                    allowAliasesToMulti = !allowAliasesToMulti;
                    mutated = true;
                }
                if (randomBoolean()) {
                    forbidClosed = !forbidClosed;
                    mutated = true;
                }
                if (randomBoolean()) {
                    ignoreAliases = !ignoreAliases;
                    mutated = true;
                }
            }
            return IndicesOptions.fromOptions(ignoreUnavailable, allowNoIndices, expandOpen, expandClosed,
                allowAliasesToMulti, forbidClosed, ignoreAliases);
        });
    }
}
