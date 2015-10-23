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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.Object;
import java.lang.String;
import java.lang.StringIndexOutOfBoundsException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;

import org.jcodings.specific.UTF8Encoding;
import org.joni.*;
import org.joni.exception.ValueException;

public class Grok {

    private HashMap<String, String> patternBank;
    private static final String NAME_GROUP = "name";
    private static final String SUBNAME_GROUP = "subname";
    private static final String PATTERN_GROUP = "pattern";
    private static final String DEFINITION_GROUP = "definition";
    private static final String GROK_PATTERN =
            "%\\{" +
            "(?<name>" +
            "(?<pattern>[A-z0-9]+)" +
            "(?::(?<subname>[A-z0-9_:.-]+))?" +
            ")" +
            "(?:=(?<definition>" +
            "(?:" +
            "(?:[^{}]+|\\.+)+" +
            ")+" +
            ")" +
            ")?" + "\\}";
    private static final Regex GROK_PATTERN_REGEX = new Regex(GROK_PATTERN.getBytes(StandardCharsets.UTF_8), 0, GROK_PATTERN.getBytes(StandardCharsets.UTF_8).length, Option.NONE, UTF8Encoding.INSTANCE, Syntax.DEFAULT);
    private Regex compiledExpression;
    private String expression;
    private boolean namedCaptures;


    public Grok() {
        this(true);
    }

    @SuppressWarnings("unchecked")
    public Grok(boolean namedCaptures) {
        this.patternBank = new HashMap<>();
        this.compiledExpression = null;
        this.expression = null;
        this.namedCaptures = namedCaptures;
    }

    /**
     * loads patterns from a directory path
     *
     * @param dir the directory to load the patterns from
     * @throws IOException when patterns are unable to be read
     */
    public void load(Path dir) throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
            for (Path path : stream) {
                try (Stream<String> lines = Files.lines(path, Charset.defaultCharset())) {
                    lines
                       .map(l -> l.replaceAll("^\\s+", ""))
                       .filter(l -> !(l.startsWith("#") || l.length() == 0))
                       .map(l -> l.split("\\s+", 2))
                       .forEachOrdered(l -> addPattern(l[0], l[1]));
                }
            }
        }
    }

    public void loadFromStream(InputStream inputStream) throws IOException {
        String line;
        BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
        while ((line = br.readLine()) != null) {
            String trimmedLine = line.replaceAll("^\\s+", "");
            if (trimmedLine.startsWith("#") || trimmedLine.length() == 0) {
                continue;
            }

            String[] parts = trimmedLine.split("\\s+", 2);
            // TODO(talevy): throw exception when line does not match pattern definition
            if (parts.length == 2) {
                addPattern(parts[0], parts[1]);
            }
        }
    }

    public void addPattern(String name, String pattern) {
        patternBank.put(name, pattern);
    }

    public String groupMatch(String name, Region region, String pattern) {
        try {
            int number = GROK_PATTERN_REGEX.nameToBackrefNumber(name.getBytes(StandardCharsets.UTF_8), 0, name.getBytes(Charset.defaultCharset()).length, region);
            int begin = region.beg[number];
            int end = region.end[number];
            return new String(pattern.getBytes(StandardCharsets.UTF_8), begin, end - begin, StandardCharsets.UTF_8);
        } catch (StringIndexOutOfBoundsException e) {
            return null;
        } catch (ValueException e) {
            return null;
        }
    }

    /**
     * converts a grok expression into a named regex expression
     *
     * @return named regex expression
     */
    public String toRegex(String grokPattern) {
        byte[] grokPatternBytes = grokPattern.getBytes(StandardCharsets.UTF_8);
        Matcher matcher = GROK_PATTERN_REGEX.matcher(grokPatternBytes);

        int result = matcher.search(0, grokPatternBytes.length, Option.NONE);
        if (result != -1) {
            Region region = matcher.getEagerRegion();
            String namedPatternRef = groupMatch(NAME_GROUP, region, grokPattern);
            String subName = groupMatch(SUBNAME_GROUP, region, grokPattern);
            // TODO(tal): Support definitions
            String definition = groupMatch(DEFINITION_GROUP, region, grokPattern);
            String patternName = groupMatch(PATTERN_GROUP, region, grokPattern);
            String pattern = patternBank.get(patternName);

            String grokPart = "";
            if (namedCaptures && subName != null) {
                grokPart = String.format(Locale.US, "(?<%s>%s)", namedPatternRef, pattern);
            } else if (!namedCaptures) {
                grokPart = String.format(Locale.US, "(?<%s>%s)", patternName + "_" + String.valueOf(result), pattern);
            } else {
                grokPart = String.format(Locale.US, "(?:%s)", pattern);
            }

            String start = new String(grokPatternBytes, 0, result, StandardCharsets.UTF_8);
            String rest = new String(grokPatternBytes, region.end[0], grokPatternBytes.length - region.end[0], StandardCharsets.UTF_8);
            return start + toRegex(grokPart + rest);
        }

        return grokPattern;
    }

    public void compile(String grokPattern) {
        expression = toRegex(grokPattern);
        byte[] expressionBytes = expression.getBytes(StandardCharsets.UTF_8);
        compiledExpression = new Regex(expressionBytes, 0, expressionBytes.length, Option.DEFAULT, UTF8Encoding.INSTANCE);
    }

    public boolean match(String text) {
        Matcher matcher = compiledExpression.matcher(text.getBytes(StandardCharsets.UTF_8));
        int result = matcher.search(0, text.length(), Option.DEFAULT);
        if (result != -1) {
            return true;
        } else {
            return false;
        }
    }

    public Map<String, Object> captures(String text) {
        byte[] textAsBytes = text.getBytes(StandardCharsets.UTF_8);
        HashMap<String, Object> fields = new HashMap<>();
        Matcher matcher = compiledExpression.matcher(textAsBytes);
        int result = matcher.search(0, textAsBytes.length, Option.DEFAULT);
        if (result != -1) {
            Region region = matcher.getEagerRegion();
            for (Iterator<NameEntry> entry = compiledExpression.namedBackrefIterator(); entry.hasNext();) {
                NameEntry e = entry.next();
                int number = e.getBackRefs()[0];

                String groupName = new String(e.name, e.nameP, e.nameEnd - e.nameP, StandardCharsets.UTF_8);
                String matchValue = null;
                if (region.beg[number] >= 0) {
                    matchValue = new String(textAsBytes, region.beg[number], region.end[number] - region.beg[number], StandardCharsets.UTF_8);
                }
                GrokMatchGroup match = new GrokMatchGroup(groupName, matchValue);
                fields.put(match.getName(), match.getValue());
            }
        } else {
            return null;
        }

        return fields;
    }
}

