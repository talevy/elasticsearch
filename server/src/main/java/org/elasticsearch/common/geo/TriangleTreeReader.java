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

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.lucene.geo.GeoUtils.orient;

/**
 * A tree reusable reader for a previous serialized {@link org.elasticsearch.geometry.Geometry} using
 * {@link TriangleTreeWriter}.
 *
 * This class supports checking bounding box
 * relations against the serialized triangle tree.
 */
public class TriangleTreeReader implements ShapeTreeReader {

    private final int extentOffset = 8;
    private ByteBufferStreamInput input;
    private final CoordinateEncoder coordinateEncoder;
    private final Rectangle2D rectangle2D;

    public TriangleTreeReader(CoordinateEncoder coordinateEncoder) {
        this.coordinateEncoder = coordinateEncoder;
        this.rectangle2D = new Rectangle2D();
    }

    public void reset(BytesRef bytesRef) {
        this.input = new ByteBufferStreamInput(ByteBuffer.wrap(bytesRef.bytes, bytesRef.offset, bytesRef.length));
    }

    /**
     * returns the bounding box of the geometry in the format [minX, maxX, minY, maxY].
     */
    public Extent getExtent() throws IOException {
        input.position(extentOffset);
        int thisMaxX = input.readInt();
        int thisMinX = Math.toIntExact(thisMaxX - input.readVLong());
        int thisMaxY = input.readInt();
        int thisMinY = Math.toIntExact(thisMaxY - input.readVLong());
        return Extent.fromPoints(thisMinX, thisMinY, thisMaxX, thisMaxY);
    }

    /**
     * returns the X coordinate of the centroid.
     */
    @Override
    public double getCentroidX() throws IOException {
        input.position(0);
        return coordinateEncoder.decodeX(input.readInt());
    }

    /**
     * returns the Y coordinate of the centroid.
     */
    @Override
    public double getCentroidY() throws IOException {
        input.position(4);
        return coordinateEncoder.decodeY(input.readInt());
    }

    /**
     * Compute the relation with the provided bounding box. If the result is CELL_INSIDE_QUERY
     * then the bounding box is within the shape.
     */
    @Override
    public GeoRelation relate(int minX, int minY, int maxX, int maxY) throws IOException {
        input.position(extentOffset);
        int thisMaxX = input.readInt();
        int thisMinX = Math.toIntExact(thisMaxX - input.readVLong());
        int thisMaxY = input.readInt();
        int thisMinY = Math.toIntExact(thisMaxY - input.readVLong());
        if (minX <= thisMinX && maxX >= thisMaxX && minY <= thisMinY && maxY >= thisMaxY) {
            // the rectangle fully contains the shape
            return GeoRelation.QUERY_CROSSES;
        }
        GeoRelation rel = GeoRelation.QUERY_DISJOINT;
        if ((thisMinX > maxX || thisMaxX < minX || thisMinY > maxY || thisMaxY < minY) == false) {
            // shapes are NOT disjoint
            rectangle2D.setValues(minX, maxX, minY, maxY);
            byte metadata = input.readByte();
            if ((metadata & 1 << 2) == 1 << 2) { // component in this node is a point
                int x = Math.toIntExact(thisMaxX - input.readVLong());
                int y = Math.toIntExact(thisMaxY - input.readVLong());
                if (rectangle2D.contains(x, y)) {
                    return GeoRelation.QUERY_CROSSES;
                }
                thisMinX = x;
            } else if ((metadata & 1 << 3) == 1 << 3) {  // component in this node is a line
                int aX = Math.toIntExact(thisMaxX - input.readVLong());
                int aY = Math.toIntExact(thisMaxY - input.readVLong());
                int bX = Math.toIntExact(thisMaxX - input.readVLong());
                int bY = Math.toIntExact(thisMaxY - input.readVLong());
                if (rectangle2D.intersectsLine(aX, aY, bX, bY)) {
                    return GeoRelation.QUERY_CROSSES;
                }
                thisMinX = aX;
            } else {  // component in this node is a triangle
                int aX = Math.toIntExact(thisMaxX - input.readVLong());
                int aY = Math.toIntExact(thisMaxY - input.readVLong());
                int bX = Math.toIntExact(thisMaxX - input.readVLong());
                int bY = Math.toIntExact(thisMaxY - input.readVLong());
                int cX = Math.toIntExact(thisMaxX - input.readVLong());
                int cY = Math.toIntExact(thisMaxY - input.readVLong());
                boolean ab = (metadata & 1 << 4) == 1 << 4;
                boolean bc = (metadata & 1 << 5) == 1 << 5;
                boolean ca = (metadata & 1 << 6) == 1 << 6;
                rel = rectangle2D.relateTriangle(aX, aY, ab, bX, bY, bc, cX, cY, ca);
                if (rel == GeoRelation.QUERY_CROSSES) {
                    return GeoRelation.QUERY_CROSSES;
                }
                thisMinX = aX;
            }
            if ((metadata & 1 << 0) == 1 << 0) { // left != null
                GeoRelation left = relate(rectangle2D, false, thisMaxX, thisMaxY);
                if (left == GeoRelation.QUERY_CROSSES) {
                    return GeoRelation.QUERY_CROSSES;
                } else if (left == GeoRelation.QUERY_INSIDE) {
                    rel = left;
                }
            }
            if ((metadata & 1 << 1) == 1 << 1) { // right != null
                if (rectangle2D.maxX >= thisMinX) {
                    GeoRelation right = relate(rectangle2D, false, thisMaxX, thisMaxY);
                    if (right == GeoRelation.QUERY_CROSSES) {
                        return GeoRelation.QUERY_CROSSES;
                    } else if (right == GeoRelation.QUERY_INSIDE) {
                        rel = right;
                    }
                }
            }
        }
        return rel;
    }

    private GeoRelation relate(Rectangle2D rectangle2D, boolean splitX, int parentMaxX, int parentMaxY) throws IOException {
        int thisMaxX = Math.toIntExact(parentMaxX - input.readVLong());
        int thisMaxY = Math.toIntExact(parentMaxY - input.readVLong());
        GeoRelation rel = GeoRelation.QUERY_DISJOINT;
        int size = input.readVInt();
        if (rectangle2D.minY <= thisMaxY && rectangle2D.minX <= thisMaxX) {
            byte metadata = input.readByte();
            int thisMinX;
            int thisMinY;
            if ((metadata & 1 << 2) == 1 << 2) { // component in this node is a point
                int x = Math.toIntExact(thisMaxX - input.readVLong());
                int y = Math.toIntExact(thisMaxY - input.readVLong());
                if (rectangle2D.contains(x, y)) {
                    return GeoRelation.QUERY_CROSSES;
                }
                thisMinX = x;
                thisMinY = y;
            } else if ((metadata & 1 << 3) == 1 << 3) { // component in this node is a line
                int aX = Math.toIntExact(thisMaxX - input.readVLong());
                int aY = Math.toIntExact(thisMaxY - input.readVLong());
                int bX = Math.toIntExact(thisMaxX - input.readVLong());
                int bY = Math.toIntExact(thisMaxY - input.readVLong());
                if (rectangle2D.intersectsLine(aX, aY, bX, bY)) {
                    return GeoRelation.QUERY_CROSSES;
                }
                thisMinX = aX;
                thisMinY = Math.min(aY, bY);
            } else { // component in this node is a triangle
                int aX = Math.toIntExact(thisMaxX - input.readVLong());
                int aY = Math.toIntExact(thisMaxY - input.readVLong());
                int bX = Math.toIntExact(thisMaxX - input.readVLong());
                int bY = Math.toIntExact(thisMaxY - input.readVLong());
                int cX = Math.toIntExact(thisMaxX - input.readVLong());
                int cY = Math.toIntExact(thisMaxY - input.readVLong());
                boolean ab = (metadata & 1 << 4) == 1 << 4;
                boolean bc = (metadata & 1 << 5) == 1 << 5;
                boolean ca = (metadata & 1 << 6) == 1 << 6;
                rel = rectangle2D.relateTriangle(aX, aY, ab, bX, bY, bc, cX, cY, ca);
                if (rel == GeoRelation.QUERY_CROSSES) {
                    return GeoRelation.QUERY_CROSSES;
                }
                thisMinX = aX;
                thisMinY = Math.min(Math.min(aY, bY), cY);
            }
            if ((metadata & 1 << 0) == 1 << 0) { // left != null
                GeoRelation left = relate(rectangle2D, !splitX, thisMaxX, thisMaxY);
                if (left == GeoRelation.QUERY_CROSSES) {
                    return GeoRelation.QUERY_CROSSES;
                } else if (left == GeoRelation.QUERY_INSIDE) {
                    rel = left;
                }
            }
            if ((metadata & 1 << 1) == 1 << 1) { // right != null
                int rightSize = input.readVInt();
                if ((splitX == false && rectangle2D.maxY >= thisMinY) || (splitX && rectangle2D.maxX >= thisMinX)) {
                    GeoRelation right = relate(rectangle2D, !splitX, thisMaxX, thisMaxY);
                    if (right == GeoRelation.QUERY_CROSSES) {
                        return GeoRelation.QUERY_CROSSES;
                    } else if (right == GeoRelation.QUERY_INSIDE) {
                        rel = right;
                    }
                } else {
                    input.skip(rightSize);
                }
            }
        } else {
            input.skip(size);
        }
        return rel;
    }

    private static class Rectangle2D {

        protected int minX;
        protected int maxX;
        protected int minY;
        protected int maxY;

        Rectangle2D() {
        }

        protected void setValues(int minX, int maxX, int minY, int maxY) {
            this.minX = minX;
            this.maxX = maxX;
            this.minY = minY;
            this.maxY = maxY;
        }

        /**
         * Checks if the rectangle contains the provided point
         **/
        public boolean contains(int x, int y) {
            return (x < minX || x > maxX || y < minY || y > maxY) == false;
        }

        /**
         * Checks if the rectangle intersects the provided triangle
         **/
        public boolean intersectsLine(int aX, int aY, int bX, int bY) {
            // 1. query contains any triangle points
            if (contains(aX, aY) || contains(bX, bY)) {
                return true;
            }

            // compute bounding box of triangle
            int tMinX = StrictMath.min(aX, bX);
            int tMaxX = StrictMath.max(aX, bX);
            int tMinY = StrictMath.min(aY, bY);
            int tMaxY = StrictMath.max(aY, bY);

            // 2. check bounding boxes are disjoint
            if (tMaxX < minX || tMinX > maxX || tMinY > maxY || tMaxY < minY) {
                return false;
            }

            // 4. last ditch effort: check crossings
            if (edgeIntersectsQuery(aX, aY, bX, bY)) {
                return true;
            }
            return false;
        }

        /**
         * Checks if the rectangle intersects the provided triangle
         **/
        public GeoRelation relateTriangle(int aX, int aY, boolean ab, int bX, int bY, boolean bc, int cX, int cY, boolean ca) {
            // 1. query contains any triangle points
            if (contains(aX, aY) || contains(bX, bY) || contains(cX, cY)) {
                return GeoRelation.QUERY_CROSSES;
            }

            // compute bounding box of triangle
            int tMinX = StrictMath.min(StrictMath.min(aX, bX), cX);
            int tMaxX = StrictMath.max(StrictMath.max(aX, bX), cX);
            int tMinY = StrictMath.min(StrictMath.min(aY, bY), cY);
            int tMaxY = StrictMath.max(StrictMath.max(aY, bY), cY);

            // 2. check bounding boxes are disjoint
            if (tMaxX < minX || tMinX > maxX || tMinY > maxY || tMaxY < minY) {
                return GeoRelation.QUERY_DISJOINT;
            }

            boolean within = false;
            if (edgeIntersectsQuery(aX, aY, bX, bY)) {
                if (ab) {
                    return GeoRelation.QUERY_CROSSES;
                }
                within = true;
            }

            // right
            if (edgeIntersectsQuery(bX, bY, cX, cY)) {
                if (bc) {
                    return GeoRelation.QUERY_CROSSES;
                }
                within = true;
            }

            if (edgeIntersectsQuery(cX, cY, aX, aY)) {
                if (ca) {
                    return GeoRelation.QUERY_CROSSES;
                }
                within = true;
            }

            if (within || pointInTriangle(tMinX, tMaxX, tMinY, tMaxY, minX, minY, aX, aY, bX, bY, cX, cY)) {
                return GeoRelation.QUERY_INSIDE;
            }

            return GeoRelation.QUERY_DISJOINT;
        }

        /**
         * returns true if the edge (defined by (ax, ay) (bx, by)) intersects the query
         */
        private boolean edgeIntersectsQuery(int ax, int ay, int bx, int by) {
            // shortcut: check bboxes of edges are disjoint
            if (boxesAreDisjoint(Math.min(ax, bx), Math.max(ax, bx), Math.min(ay, by), Math.max(ay, by),
                minX, maxX, minY, maxY)) {
                return false;
            }

            // top
            if (orient(ax, ay, bx, by, minX, maxY) * orient(ax, ay, bx, by, maxX, maxY) <= 0 &&
                orient(minX, maxY, maxX, maxY, ax, ay) * orient(minX, maxY, maxX, maxY, bx, by) <= 0) {
                return true;
            }

            // right
            if (orient(ax, ay, bx, by, maxX, maxY) * orient(ax, ay, bx, by, maxX, minY) <= 0 &&
                orient(maxX, maxY, maxX, minY, ax, ay) * orient(maxX, maxY, maxX, minY, bx, by) <= 0) {
                return true;
            }

            // bottom
            if (orient(ax, ay, bx, by, maxX, minY) * orient(ax, ay, bx, by, minX, minY) <= 0 &&
                orient(maxX, minY, minX, minY, ax, ay) * orient(maxX, minY, minX, minY, bx, by) <= 0) {
                return true;
            }

            // left
            if (orient(ax, ay, bx, by, minX, minY) * orient(ax, ay, bx, by, minX, maxY) <= 0 &&
                orient(minX, minY, minX, maxY, ax, ay) * orient(minX, minY, minX, maxY, bx, by) <= 0) {
                return true;
            }

            return false;
        }

        /**
         * Compute whether the given x, y point is in a triangle; uses the winding order method
         */
        static boolean pointInTriangle(double minX, double maxX, double minY, double maxY, double x, double y,
                                       double aX, double aY, double bX, double bY, double cX, double cY) {
            //check the bounding box because if the triangle is degenerated, e.g points and lines, we need to filter out
            //coplanar points that are not part of the triangle.
            if (x >= minX && x <= maxX && y >= minY && y <= maxY) {
                int a = orient(x, y, aX, aY, bX, bY);
                int b = orient(x, y, bX, bY, cX, cY);
                if (a == 0 || b == 0 || a < 0 == b < 0) {
                    int c = orient(x, y, cX, cY, aX, aY);
                    return c == 0 || (c < 0 == (b < 0 || a < 0));
                }
                return false;
            } else {
                return false;
            }
        }

        /**
         * utility method to check if two boxes are disjoint
         */
        private static boolean boxesAreDisjoint(final int aMinX, final int aMaxX, final int aMinY, final int aMaxY,
                                                final int bMinX, final int bMaxX, final int bMinY, final int bMaxY) {
            return (aMaxX < bMinX || aMinX > bMaxX || aMaxY < bMinY || aMinY > bMaxY);
        }

    }
}
