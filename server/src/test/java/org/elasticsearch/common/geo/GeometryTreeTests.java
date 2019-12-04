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

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.index.fielddata.MultiGeoValues;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.search.aggregations.bucket.geogrid.CellIdSource;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoGridTiler;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;


public class GeometryTreeTests extends AbstractTreeTestCase {

    protected ShapeTreeReader geometryTreeReader(Geometry geometry, CoordinateEncoder encoder) throws IOException {
        GeometryTreeWriter writer = new GeometryTreeWriter(geometry, encoder);
        BytesStreamOutput output = new BytesStreamOutput();
        writer.writeTo(output);
        output.close();
        GeometryTreeReader reader = new GeometryTreeReader(encoder);
        reader.reset(output.bytes().toBytesRef());
        return reader;
    }

    public void testWhat() throws Exception {
        Geometry geometry;
        ShapeTreeReader geometryTreeReader;
        ShapeTreeReader triangleTreeReader;
        MultiGeoValues.GeoShapeValue geometryTreeValue;
        MultiGeoValues.GeoShapeValue triangleTreeValue;
        CellIdSource.GeoShapeCellValues cellValues;
        XContentParser fijiParser = XContentHelper.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            new BytesArray(
                "{\n" + "  \"type\": \"Polygon\",\n" + "  \"coordinates\": [\n" + "   [\n" + "    [\n" + "     9.594226,\n" + "     47.525058\n" + "    ],\n" + "    [\n" + "     9.632932,\n" + "     47.347601\n" + "    ],\n" + "    [\n" + "     9.47997,\n" + "     47.10281\n" + "    ],\n" + "    [\n" + "     9.932448,\n" + "     46.920728\n" + "    ],\n" + "    [\n" + "     10.442701,\n" + "     46.893546\n" + "    ],\n" + "    [\n" + "     10.363378,\n" + "     46.483571\n" + "    ],\n" + "    [\n" + "     9.922837,\n" + "     46.314899\n" + "    ],\n" + "    [\n" + "     9.182882,\n" + "     46.440215\n" + "    ],\n" + "    [\n" + "     8.966306,\n" + "     46.036932\n" + "    ],\n" + "    [\n" + "     8.489952,\n" + "     46.005151\n" + "    ],\n" + "    [\n" + "     8.31663,\n" + "     46.163642\n" + "    ],\n" + "    [\n" + "     7.755992,\n" + "     45.82449\n" + "    ],\n" + "    [\n" + "     7.273851,\n" + "     45.776948\n" + "    ],\n" + "    [\n" + "     6.843593,\n" + "     45.991147\n" + "    ],\n" + "    [\n" + "     6.5001,\n" + "     46.429673\n" + "    ],\n" + "    [\n" + "     6.022609,\n" + "     46.27299\n" + "    ],\n" + "    [\n" + "     6.037389,\n" + "     46.725779\n" + "    ],\n" + "    [\n" + "     6.768714,\n" + "     47.287708\n" + "    ],\n" + "    [\n" + "     6.736571,\n" + "     47.541801\n" + "    ],\n" + "    [\n" + "     7.192202,\n" + "     47.449766\n" + "    ],\n" + "    [\n" + "     7.466759,\n" + "     47.620582\n" + "    ],\n" + "    [\n" + "     8.317301,\n" + "     47.61358\n" + "    ],\n" + "    [\n" + "     8.522612,\n" + "     47.830828\n" + "    ],\n" + "    [\n" + "     9.594226,\n" + "     47.525058\n" + "    ]\n" + "   ]\n" + "  ]\n" + " }"
            ), XContentType.JSON);

        fijiParser.nextToken();
        Geometry fiji = new GeometryParser(true, true, true).parse(fijiParser);
        geometry = new GeoShapeIndexer(true, "indexer").prepareForIndexing(fiji);

        GeoShapeIndexer indexer = new GeoShapeIndexer(true, "test");
        geometry = indexer.prepareForIndexing(geometry);

        GeoShapeCoordinateEncoder encoder = GeoShapeCoordinateEncoder.INSTANCE;
        geometryTreeReader = geometryTreeReader(geometry, encoder);
        triangleTreeReader = triangleTreeReader(geometry, encoder);
        geometryTreeValue = new MultiGeoValues.GeoShapeValue(geometryTreeReader);
        triangleTreeValue = new MultiGeoValues.GeoShapeValue(triangleTreeReader);

        int precision = 9;
        cellValues = new CellIdSource.GeoShapeCellValues(null, precision, GeoGridTiler.GeoTileGridTiler.INSTANCE);

        MultiGeoValues.BoundingBox bbox1 = geometryTreeValue.boundingBox();
        MultiGeoValues.BoundingBox bbox2 = triangleTreeValue.boundingBox();
        assertThat(bbox1.minX(), equalTo(bbox2.minX()));
        assertThat(bbox1.maxX(), equalTo(bbox2.maxX()));
        assertThat(bbox1.minY(), equalTo(bbox2.minY()));
        assertThat(bbox1.maxY(), equalTo(bbox2.maxY()));

        {
            int v = GeoGridTiler.GeoTileGridTiler.INSTANCE.setValues(cellValues, geometryTreeValue, precision);
            assertThat(v, equalTo(28));
        }
//
//        {
//            int v = GeoGridTiler.GeoTileGridTiler.INSTANCE.setValues(cellValues, triangleTreeValue, precision);
//            assertThat(v, equalTo(28));
//        }
    }

    public static TriangleTreeReader triangleTreeReader(Geometry geometry, CoordinateEncoder encoder) throws IOException {
        TriangleTreeWriter writer = new TriangleTreeWriter(geometry, encoder);
        BytesStreamOutput output = new BytesStreamOutput();
        writer.writeTo(output);
        output.close();
        TriangleTreeReader reader = new TriangleTreeReader(encoder);
        reader.reset(output.bytes().toBytesRef());
        return reader;
    }
}
