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

import org.apache.lucene.geo.Polygon2D;
import org.apache.lucene.geo.Polygon2DUtils;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.geo.geometry.Geometry;
import org.elasticsearch.geo.geometry.Polygon;

import java.util.ArrayList;
import java.util.List;

// TODO(talevy): support other geometries besides Polygon, and besides single polygon without holes
public class BinaryPolygon2DDocValuesField extends CustomDocValuesField {

    private List<org.apache.lucene.geo.Polygon> lucenePolygons;

    public BinaryPolygon2DDocValuesField(String name, Geometry geometry) {
        super(name);
        this.lucenePolygons = new ArrayList<>(1);
        lucenePolygons.add(GeoShapeFieldMapper.toLucenePolygon((Polygon) geometry));
    }

    public void add(Geometry geometry) {
        lucenePolygons.add(GeoShapeFieldMapper.toLucenePolygon((Polygon) geometry));
    }

    Polygon2D polygon2D() {
        Polygon2D polygon2D;
        if (lucenePolygons.size() > 1) {
            org.apache.lucene.geo.Polygon[] arr = new org.apache.lucene.geo.Polygon[lucenePolygons.size()];
            lucenePolygons.toArray(arr);
            polygon2D = Polygon2D.create(arr);
        } else {
            polygon2D = Polygon2D.create(lucenePolygons.get(0));
        }
        return polygon2D;
    }

    @Override
    public BytesRef binaryValue() {
        try {
            return Polygon2DUtils.polygon2DToBytes(polygon2D());
        } catch (Exception e) {
            throw new ElasticsearchException("failed to encode polygon", e);
        }
    }
}
