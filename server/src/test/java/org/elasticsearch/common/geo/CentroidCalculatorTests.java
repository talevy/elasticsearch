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
package org.elasticsearch.common.geo;

import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;

public class CentroidCalculatorTests extends ESTestCase {

    public void testPoints() {
        double[] x = new double[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        double[] y = new double[] { 10, 20, 30, 40, 50, 60, 70, 80, 90, 100 };
        double[] xRunningAvg = new double[] { 1, 1.5, 2.0, 2.5, 3, 3.5, 4, 4.5, 5, 5.5 };
        double[] yRunningAvg = new double[] { 10, 15, 20, 25, 30, 35, 40, 45, 50, 55 };
        CentroidCalculator calculator = new CentroidCalculator();
        for (int i = 0; i < 10; i++) {
            calculator.addPoint(new Point(x[i], y[i]));
            assertThat(calculator.getX(), equalTo(xRunningAvg[i]));
            assertThat(calculator.getY(), equalTo(yRunningAvg[i]));
        }
        CentroidCalculator otherCalculator = new CentroidCalculator();
        otherCalculator.addPoint(new Point(0.0, 0.0));
        calculator.addFrom(otherCalculator);
        assertThat(calculator.getX(), equalTo(5.0));
        assertThat(calculator.getY(), equalTo(50.0));
    }

    public void testLines() {
        double[] x = new double[] { 1, 2, 3, 5 };
        double[] y = new double[] { 10, 20, 30, 50 };
        Line line = new Line(x, y);
        CentroidCalculator calculator = new CentroidCalculator();
        calculator.addLine(line);
        assertThat(calculator.getX(), equalTo(3.0));
        assertThat(calculator.getY(), equalTo(30.0));
    }

    public void testPolygon() {
        double[] px = {0, 10, 10, 20, 20, 30, 30, 40, 40, 50, 50, 0, 0};
        double[] py = {0, 0, 20, 20, 0, 0, 20, 20, 0, 0, 30, 30, 0};

        double[] hx = {21, 21, 29, 29, 21};
        double[] hy = {1, 20, 20, 1, 1};

        Polygon polyWithHole = new Polygon(new LinearRing(px, py), Collections.singletonList(new LinearRing(hx, hy)));
        CentroidCalculator calculator = new CentroidCalculator();
        calculator.addPolygon(polyWithHole);
        assertThat(calculator.getX(), equalTo(3.0));
        assertThat(calculator.getY(), equalTo(30.0));
    }

    public void testPolygonAndMore() {
        // TODO
    }

    public void testLinesAndPoints() {
        // TODO
    }
}
