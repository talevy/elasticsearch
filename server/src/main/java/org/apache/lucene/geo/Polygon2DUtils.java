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
package org.apache.lucene.geo;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Queue;

public class Polygon2DUtils {

    /**
     *
     * @param polygon2D Polygon2D to serialize, assumes balanced tree
     * @return serialized polygon2D
     */
    public static BytesRef polygon2DToBytes(Polygon2D polygon2D) throws IOException {
        if (polygon2D.left != null || polygon2D.right != null) {
            throw new IllegalArgumentException("only supports one polygon... and ignores holes because knowledge is power");
        }
        BytesStreamOutput output = new BytesStreamOutput();
        bfsSerializePolygon2D(output, polygon2D);
        output.close();
        return output.bytes().toBytesRef();
    }

    public static Polygon2D bytesToPolygon2D(BytesRef bytesRef) throws IOException {
        StreamInput in = ByteBufferStreamInput.wrap(bytesRef.bytes);
        double minLon = in.readDouble();
        double maxLon = in.readDouble();
        double minLat = in.readDouble();
        double maxLat = in.readDouble();
        EdgeTree.Edge[] edges = bytesToEdgeTree(in);
        int numEdges = (int) Arrays.stream(edges).filter((e) -> e.low != Double.MAX_VALUE).count();
        double[] polyLats = new double[numEdges + 1];
        double[] polyLons = new double[numEdges + 1];

        Queue<EdgeTree.Edge> queue = new LinkedList<>();
        queue.add(edges[0]);
        EdgeTree.Edge cur = queue.poll();
        int idx = 0;
        while (cur != null) {
            polyLats[idx] = cur.lat1;
            polyLons[idx] = cur.lon1;
            if (cur.lat2 == polyLats[0] && cur.lon2 == polyLons[0]) {
                // closed poly
                assert idx + 1 == numEdges;
                polyLats[idx+1] = cur.lat2;
                polyLons[idx+1] = cur.lon2;
                break;
            }
            for (int i = 0; i < edges.length; i++) {
                if (edges[i].low == Double.MAX_VALUE) {
                    continue;
                }
                if (cur.lat2 == edges[i].lat1 && cur.lon2 == edges[i].lon1) {
                    queue.add(edges[i]);
                    break;
                }
            }
            cur = queue.poll();
            idx += 1;
        }

        in.close();
        return Polygon2D.create(new Polygon(polyLats, polyLons));
    }

    private static EdgeTree.Edge[] bytesToEdgeTree(StreamInput in) throws IOException {
        int numEdges = in.readVInt();
        EdgeTree.Edge[] edges = new EdgeTree.Edge[numEdges];
        int i = 0;
        while (i < numEdges) {
            double low = in.readDouble();
            double max = in.readDouble();
            double lon1 = in.readDouble();
            double lon2 = in.readDouble();
            double lat1 = in.readDouble();
            double lat2 = in.readDouble();
            EdgeTree.Edge edge = new EdgeTree.Edge(lat1, lon1, lat2, lon2, low, max);
            edges[i++] = edge;
        }
        return edges;
    }

    static void bfsSerializePolygon2D(BytesStreamOutput output, Polygon2D poly2D) throws IOException {
        Queue<Polygon2D> queue = new LinkedList<>();
        queue.add(poly2D);
        Polygon2D tree = queue.poll();
        while (tree != null) {
            output.writeDouble(poly2D.minLon);
            output.writeDouble(poly2D.maxLon);
            output.writeDouble(poly2D.minLat);
            output.writeDouble(poly2D.maxLat);
            bfsSerializeEdges(output, poly2D.tree);

            if (tree.left != null) {
                queue.add((Polygon2D) tree.left);
            }
            if (tree.right != null) {
                queue.add((Polygon2D) tree.right);
            }

            tree = queue.poll();
        }
    }

    private static int numNodes(EdgeTree.Edge edge) {
        int num = 1;
        if (edge.left != null) {
            num += numNodes(edge.left);
        }
        if (edge.right != null) {
            num += numNodes(edge.right);
        }
        return num;
    }

    static void bfsSerializeEdges(BytesStreamOutput output, EdgeTree.Edge edge) throws IOException {
        int numCompleteNodes = (int) Math.pow(2, Math.ceil(Math.log(numNodes(edge) + 1)/Math.log(2))) - 1;

        output.writeVInt(numCompleteNodes);
        Queue<EdgeTree.Edge> queue = new LinkedList<>();
        queue.add(edge);
        EdgeTree.Edge tree = queue.poll();
        int i = 0;
        while (tree != null) {
            i += 1;
            output.writeDouble(tree.low);
            output.writeDouble(tree.max);
            output.writeDouble(tree.lon1);
            output.writeDouble(tree.lon2);
            output.writeDouble(tree.lat1);
            output.writeDouble(tree.lat2);

            if (i == numCompleteNodes) {
                break;
            }

            if (tree.low == Double.MAX_VALUE) {
                tree = queue.poll();
                continue;
            }

            if (tree.left != null) {
                queue.add(tree.left);
            } else {
                // serialize dummy because of offset in byte array
                queue.add(new EdgeTree.Edge(Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE,
                    Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE));
            }
            if (tree.right != null) {
                queue.add(tree.right);
            } else {
                // serialize dummy because of offset in byte array
                queue.add(new EdgeTree.Edge(Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE,
                    Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE));
            }

            tree = queue.poll();
        }
        assert i == numCompleteNodes;
    }
}
