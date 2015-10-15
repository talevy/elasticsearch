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

import org.elasticsearch.ingest.processor.grok.Grok;
import org.elasticsearch.ingest.processor.grok.GrokProcessor;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.Object;import java.lang.String;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;


public class GrokTests extends ESTestCase {

    private Grok baseGrok;

    @Before
    public void setup() {
        baseGrok = new Grok();
        try {
            InputStream is = getClass().getResourceAsStream(GrokProcessor.PATTERNS_PATH);
            BufferedReader br = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
            String fileNameInLine;
            while ((fileNameInLine = br.readLine()) != null) {
                baseGrok.loadFromStream(getClass().getResourceAsStream(GrokProcessor.PATTERNS_PATH + "/" + fileNameInLine));
            }
        } catch (IOException e) { }
    }

    public void testSimpleSyslogLine() {
        String line = "Mar 16 00:01:25 evita postfix/smtpd[1713]: connect from camomile.cloud9.net[168.100.1.3]";
        baseGrok.compile("%{SYSLOGLINE}");
        Map<String, Object> matches = baseGrok.captures(line);
        assertEquals("evita", matches.get("logsource"));
        assertEquals("Mar 16 00:01:25", matches.get("timestamp"));
        assertEquals("connect from camomile.cloud9.net[168.100.1.3]", matches.get("message"));
        assertEquals("postfix/smtpd", matches.get("program"));
        assertEquals("1713", matches.get("pid"));
    }

    public void testSyslog5424Line() {
        String line = "<191>1 2009-06-30T18:30:00+02:00 paxton.local grokdebug 4123 - [id1 foo=\\\"bar\\\"][id2 baz=\\\"something\\\"] Hello, syslog.";
        baseGrok.compile("%{SYSLOG5424LINE}");
        Map<String, Object> matches = baseGrok.captures(line);
        assertEquals("191", matches.get("syslog5424_pri"));
        assertEquals("1", matches.get("syslog5424_ver"));
        assertEquals("2009-06-30T18:30:00+02:00", matches.get("syslog5424_ts"));
        assertEquals("paxton.local", matches.get("syslog5424_host"));
        assertEquals("grokdebug", matches.get("syslog5424_app"));
        assertEquals("4123", matches.get("syslog5424_proc"));
        assertEquals(null, matches.get("syslog5424_msgid"));
        assertEquals("[id1 foo=\\\"bar\\\"][id2 baz=\\\"something\\\"]", matches.get("syslog5424_sd"));
        assertEquals("Hello, syslog.", matches.get("syslog5424_msg"));
    }

    public void testDatePattern() {
        String line = "fancy 12-12-12 12:12:12";
        baseGrok.compile("(?<timestamp>%{DATE_EU} %{TIME})");
        Map<String, Object> matches = baseGrok.captures(line);
        assertEquals("12-12-12 12:12:12", matches.get("timestamp"));
    }

    public void testNilCoercedValues() {
        baseGrok.compile("test (N/A|%{BASE10NUM:duration:float}ms)");
        Map<String, Object> matches = baseGrok.captures("test 28.4ms");
        assertEquals(28.4f, matches.get("duration"));
        matches = baseGrok.captures("test N/A");
        assertEquals(null, matches.get("duration"));
    }

    public void testNilWithNoCoercion() {
        baseGrok.compile("test (N/A|%{BASE10NUM:duration}ms)");
        Map<String, Object> matches = baseGrok.captures("test 28.4ms");
        assertEquals("28.4", matches.get("duration"));
        matches = baseGrok.captures("test N/A");
        assertEquals(null, matches.get("duration"));
    }

    public void testNoNamedCaptures() {
        Grok g = new Grok(false);
        g.addPattern("NAME", "Tal");
        g.addPattern("EXCITED_NAME", "!!!%{NAME:name}!!!");
        g.addPattern("TEST", "hello world");

        String text = "wowza !!!Tal!!! - Tal";
        String pattern = "%{EXCITED_NAME} - %{NAME}";
        g.compile(pattern);

        assertEquals("(?<EXCITED_NAME_0>!!!(?<NAME_21>Tal)!!!) - (?<NAME_22>Tal)", g.toRegex(pattern));
        assertEquals(true, g.match(text));

        Object actual = g.captures(text);
        HashMap<String, Object> expected = new HashMap<>();
        expected.put("EXCITED_NAME_0", "!!!Tal!!!");
        expected.put("NAME_21", "Tal");
        expected.put("NAME_22", "Tal");
        assertEquals(expected, actual);
    }

    public void testNumericCapturesCoercion() {
        Grok g = new Grok();
        g.addPattern("BASE10NUM", "(?<![0-9.+-])(?>[+-]?(?:(?:[0-9]+(?:\\.[0-9]+)?)|(?:\\.[0-9]+)))");
        g.addPattern("NUMBER", "(?:%{BASE10NUM})");

        String pattern = "%{NUMBER:bytes:float} %{NUMBER:status} %{NUMBER}";
        g.compile(pattern);

        String text = "12009.34 200 9032";
        HashMap<String, Object> expected = new HashMap<>();
        expected.put("bytes", 12009.34f);
        expected.put("status", "200");
        Map<String, Object> actual = g.captures(text);

        assertEquals(expected, actual);
    }

    public void testApacheLog() {
        String logLine = "31.184.238.164 - - [24/Jul/2014:05:35:37 +0530] \"GET /logs/access.log HTTP/1.0\" 200 69849 \"http://8rursodiol.enjin.com\" \"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/30.0.1599.12785 YaBrowser/13.12.1599.12785 Safari/537.36\" \"www.dlwindianrailways.com\"";
        baseGrok.compile("%{COMBINEDAPACHELOG}");
        Map<String, Object> matches = baseGrok.captures(logLine);

        assertEquals("31.184.238.164", matches.get("clientip"));
        assertEquals("-", matches.get("ident"));
        assertEquals("-", matches.get("auth"));
        assertEquals("24/Jul/2014:05:35:37 +0530", matches.get("timestamp"));
        assertEquals("GET", matches.get("verb"));
        assertEquals("/logs/access.log", matches.get("request"));
        assertEquals("1.0", matches.get("httpversion"));
        assertEquals("200", matches.get("response"));
        assertEquals("69849", matches.get("bytes"));
        assertEquals("\"http://8rursodiol.enjin.com\"", matches.get("referrer"));
        assertEquals(null, matches.get("port"));
        assertEquals("\"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/30.0.1599.12785 YaBrowser/13.12.1599.12785 Safari/537.36\"", matches.get("agent"));
    }

    public void testComplete() {
        Grok g = new Grok();
        g.addPattern("MONTHDAY", "(?:(?:0[1-9])|(?:[12][0-9])|(?:3[01])|[1-9])");
        g.addPattern("MONTH", "\\b(?:Jan(?:uary|uar)?|Feb(?:ruary|ruar)?|M(?:a|ä)?r(?:ch|z)?|Apr(?:il)?|Ma(?:y|i)?|Jun(?:e|i)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|O(?:c|k)?t(?:ober)?|Nov(?:ember)?|De(?:c|z)(?:ember)?)\\b");
        g.addPattern("MINUTE", "(?:[0-5][0-9])");
        g.addPattern("YEAR", "(?>\\d\\d){1,2}");
        g.addPattern("HOUR", "(?:2[0123]|[01]?[0-9])");
        g.addPattern("SECOND", "(?:(?:[0-5]?[0-9]|60)(?:[:.,][0-9]+)?)");
        g.addPattern("TIME", "(?!<[0-9])%{HOUR}:%{MINUTE}(?::%{SECOND})(?![0-9])");
        g.addPattern("INT", "(?:[+-]?(?:[0-9]+))");
        g.addPattern("HTTPDATE", "%{MONTHDAY}/%{MONTH}/%{YEAR}:%{TIME} %{INT}");
        g.addPattern("WORD", "\\b\\w+\\b");
        g.addPattern("BASE10NUM", "(?<![0-9.+-])(?>[+-]?(?:(?:[0-9]+(?:\\.[0-9]+)?)|(?:\\.[0-9]+)))");
        g.addPattern("NUMBER", "(?:%{BASE10NUM})");
        g.addPattern("IPV6", "((([0-9A-Fa-f]{1,4}:){7}([0-9A-Fa-f]{1,4}|:))|(([0-9A-Fa-f]{1,4}:){6}(:[0-9A-Fa-f]{1,4}|((25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)(\\.(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)){3})|:))|(([0-9A-Fa-f]{1,4}:){5}(((:[0-9A-Fa-f]{1,4}){1,2})|:((25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)(\\.(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)){3})|:))|(([0-9A-Fa-f]{1,4}:){4}(((:[0-9A-Fa-f]{1,4}){1,3})|((:[0-9A-Fa-f]{1,4})?:((25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)(\\.(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)){3}))|:))|(([0-9A-Fa-f]{1,4}:){3}(((:[0-9A-Fa-f]{1,4}){1,4})|((:[0-9A-Fa-f]{1,4}){0,2}:((25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)(\\.(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)){3}))|:))|(([0-9A-Fa-f]{1,4}:){2}(((:[0-9A-Fa-f]{1,4}){1,5})|((:[0-9A-Fa-f]{1,4}){0,3}:((25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)(\\.(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)){3}))|:))|(([0-9A-Fa-f]{1,4}:){1}(((:[0-9A-Fa-f]{1,4}){1,6})|((:[0-9A-Fa-f]{1,4}){0,4}:((25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)(\\.(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)){3}))|:))|(:(((:[0-9A-Fa-f]{1,4}){1,7})|((:[0-9A-Fa-f]{1,4}){0,5}:((25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)(\\.(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)){3}))|:)))(%.+)?");
        g.addPattern("IPV4", "(?<![0-9])(?:(?:[0-1]?[0-9]{1,2}|2[0-4][0-9]|25[0-5])[.](?:[0-1]?[0-9]{1,2}|2[0-4][0-9]|25[0-5])[.](?:[0-1]?[0-9]{1,2}|2[0-4][0-9]|25[0-5])[.](?:[0-1]?[0-9]{1,2}|2[0-4][0-9]|25[0-5]))(?![0-9])");
        g.addPattern("IP", "(?:%{IPV6}|%{IPV4})");
        g.addPattern("HOSTNAME", "\\b(?:[0-9A-Za-z][0-9A-Za-z-]{0,62})(?:\\.(?:[0-9A-Za-z][0-9A-Za-z-]{0,62}))*(\\.?|\\b)");
        g.addPattern("IPORHOST", "(?:%{IP}|%{HOSTNAME})");
        g.addPattern("USER", "[a-zA-Z0-9._-]+");
        g.addPattern("DATA", ".*?");
        g.addPattern("QS", "(?>(?<!\\\\)(?>\"(?>\\\\.|[^\\\\\"]+)+\"|\"\"|(?>'(?>\\\\.|[^\\\\']+)+')|''|(?>`(?>\\\\.|[^\\\\`]+)+`)|``))");

        String text = "83.149.9.216 - - [19/Jul/2015:08:13:42 +0000] \"GET /presentations/logstash-monitorama-2013/images/kibana-dashboard3.png HTTP/1.1\" 200 171717 \"http://semicomplete.com/presentations/logstash-monitorama-2013/\" \"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1700.77 Safari/537.36\"";
        String pattern = "%{IPORHOST:clientip} %{USER:ident} %{USER:auth} \\[%{HTTPDATE:timestamp}\\] \"%{WORD:verb} %{DATA:request} HTTP/%{NUMBER:httpversion}\" %{NUMBER:response:int} (?:-|%{NUMBER:bytes:int}) %{QS:referrer} %{QS:agent}";

        g.compile(pattern);

        HashMap<String, Object> expected = new HashMap<>();
        expected.put("clientip", "83.149.9.216");
        expected.put("ident", "-");
        expected.put("auth", "-");
        expected.put("timestamp", "19/Jul/2015:08:13:42 +0000");
        expected.put("verb", "GET");
        expected.put("request", "/presentations/logstash-monitorama-2013/images/kibana-dashboard3.png");
        expected.put("httpversion", "1.1");
        expected.put("response", 200);
        expected.put("bytes", 171717);
        expected.put("referrer", "\"http://semicomplete.com/presentations/logstash-monitorama-2013/\"");
        expected.put("agent", "\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1700.77 Safari/537.36\"");

        Map<String, Object> actual = g.captures(text);

        assertEquals(expected, actual);
    }
}
