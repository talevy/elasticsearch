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
import org.elasticsearch.geo.geometry.ShapeType;

import java.io.IOException;
import java.nio.ByteBuffer;

public class GeometryTreeReader {

    private final BytesRef bytesRef;

    public GeometryTreeReader(BytesRef bytesRef) {
        this.bytesRef = bytesRef;
    }

    public boolean containedInOrCrosses(int minLon, int minLat, int maxLon, int maxLat) throws IOException {
        ByteBufferStreamInput input = new ByteBufferStreamInput(
            ByteBuffer.wrap(bytesRef.bytes, bytesRef.offset, bytesRef.length));
        boolean hasExtent = input.readBoolean();
        if (hasExtent) {
            int thisMinLon = input.readInt();
            int thisMinLat = input.readInt();
            int thisMaxLon = input.readInt();
            int thisMaxLat = input.readInt();

            if (thisMinLat > maxLat || thisMaxLon < minLon || thisMaxLat < minLat || thisMinLon > maxLon) {
                return false; // tree and bbox-query are disjoint
            }

            if (minLon <= thisMinLon && minLat <= thisMinLat && maxLon >= thisMaxLon && maxLat >= thisMaxLat) {
                return true; // bbox-query fully contains tree's extent.
            }
        }

        int numTrees = input.readVInt();
        for (int i = 0; i < numTrees; i++) {
            ShapeType shapeType = input.readEnum(ShapeType.class);
            if (ShapeType.POLYGON.equals(shapeType)) {
                BytesRef treeRef = input.readBytesRef();
                EdgeTreeReader reader = new EdgeTreeReader(treeRef);
                if (reader.containedInOrCrosses(minLon, minLat, maxLon, maxLat)) {
                    return true;
                }
            }
        }
        return false;
    }
}
