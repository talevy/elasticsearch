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

package org.elasticsearch.index.mapper;

    import org.apache.lucene.util.BytesRef;
    import org.elasticsearch.ElasticsearchException;
    import org.elasticsearch.common.geo.GeometryTreeWriter;
    import org.elasticsearch.geo.geometry.Geometry;

    import java.util.ArrayList;
    import java.util.List;

// TODO(talevy): support other geometries besides Polygon, and besides single polygon without holes
public class BinaryGeoShapeDocValueField extends CustomDocValuesField {

    private List<Geometry> shapes;

    public BinaryGeoShapeDocValueField(String name, Geometry geometry) {
        super(name);
        this.shapes = new ArrayList<>(1);
        shapes.add(geometry);
    }

    public void add(Geometry geometry) {
        shapes.add(geometry);
    }

    @Override
    public BytesRef binaryValue() {
        try {
            GeometryTreeWriter writer = new GeometryTreeWriter(shapes.get(0));
            return writer.toBytesRef();
        } catch (Exception e) {
            throw new ElasticsearchException("failed to encode polygon", e);
        }
    }
}
