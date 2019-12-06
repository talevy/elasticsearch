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
import org.elasticsearch.geometry.Rectangle;

/**
 * This class keeps a running Kahan-sum of coordinates
 * that are to be averaged in {@link TriangleTreeWriter} for use
 * as the centroid of a shape.
 */
public class CentroidCalculator {

    private double compX;
    private double compY;
    private double sumX;
    private double sumY;
    private double sumWeight;
    private int count;

    // TODO(keep track of highest dimension)
    public CentroidCalculator() {
        this.sumX = 0.0;
        this.compX = 0.0;
        this.sumY = 0.0;
        this.compY = 0.0;
        this.count = 0;
        this.sumWeight = 0;
    }

    /**
     * @return the x-coordinate centroid
     */
    public double getX() {
        return sumX / sumWeight;
    }

    /**
     * @return the y-coordinate centroid
     */
    public double getY() {
        return sumY / sumWeight;
    }

    public void addLine(Line line) {
        for (int i = 0; i < line.length() - 1; i++) {
            double diffX = line.getX(i) - line.getX(i + 1);
            double diffY = line.getY(i) - line.getY(i + 1);
            double weight = Math.sqrt(diffX * diffX + diffY * diffY);
            double x = weight * (line.getX(i) + line.getX(i + 1)) / 2;
            double y = weight * (line.getY(i) + line.getY(i + 1)) / 2;
            addWeightedCoordinate(x, y, weight);
        }
    }

    public void addPolygon(Polygon polygon) {
        addLinearRing(polygon.getPolygon(), 1);
        for (int i = 0; i < polygon.getNumberOfHoles(); i++) {
            addLinearRing(polygon.getHole(i),  -1);
        }
    }

    private void addLinearRing(LinearRing ring, int sign) {
        for (int i = 0; i < ring.length() - 1; i++) {
            double weight = ring.getX(i) * ring.getY(i + 1) + ring.getY(i) * ring.getX(i + 1);
            addWeightedCoordinate((ring.getX(i) + ring.getX(i + 1)) / 2, (ring.getY(i) + ring.getY(i + 1)) / 2, sign * weight);
        }
    }

    public void addRectangle(Rectangle rectangle) {
        double diffX = rectangle.getMaxX() - rectangle.getMinX();
        double diffY = rectangle.getMaxY() - rectangle.getMinY();
        addWeightedCoordinate(diffX / 2, diffY / 2, diffX * diffY);
    }

    public void addPoint(Point point) {
        addWeightedCoordinate(point.getX(), point.getY(), 1);
    }

    /**
     * adds a single coordinate to the running sum and count of coordinates
     * for centroid calculation
     *
     * @param x the x-coordinate of the point
     * @param y the y-coordinate of the point
     * @param weight the weight of the point
     */
    public void addWeightedCoordinate(double x, double y, double weight) {
        double correctedX = x - compX;
        double newSumX = sumX + correctedX;
        compX = (newSumX - sumX) - correctedX;
        sumX = newSumX;

        double correctedY = y - compY;
        double newSumY = sumY + correctedY;
        compY = (newSumY - sumY) - correctedY;
        sumY = newSumY;

        count += 1;
        sumWeight += weight;
    }

    /**
     * Adjusts the existing calculator to add the running sum and count
     * from another {@link CentroidCalculator}. This is used to keep
     * a running count of points from different sub-shapes of a single
     * geo-shape field
     *
     * @param otherCalculator the other centroid calculator to add from
     */
    void addFrom(CentroidCalculator otherCalculator) {
        addWeightedCoordinate(otherCalculator.sumX, otherCalculator.sumY, otherCalculator.sumWeight);
        // adjust count
        count += otherCalculator.count - 1;
    }
}
