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
package org.elasticsearch.benchmark.search.aggregations;


import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.geo.CoordinateEncoder;
import org.elasticsearch.common.geo.Extent;
import org.elasticsearch.common.geo.GeoJson;
import org.elasticsearch.common.geo.GeoShapeCoordinateEncoder;
import org.elasticsearch.common.geo.GeometryParser;
import org.elasticsearch.common.geo.GeometryTreeReader;
import org.elasticsearch.common.geo.GeometryTreeWriter;
import org.elasticsearch.common.geo.TriangleTreeReader;
import org.elasticsearch.common.geo.TriangleTreeWriter;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.index.fielddata.MultiGeoValues;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.search.aggregations.bucket.geogrid.CellIdSource;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoGridTiler;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

@Warmup(iterations = 3)
@Measurement(iterations = 1000)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
@Fork(value = 2)
public class GeoTileGridBenchmark {

    int precision;
    Geometry geometry;
    GeometryTreeReader geometryTreeReader;
    TriangleTreeReader triangleTreeReader;
    MultiGeoValues.GeoShapeValue geometryTreeValue;
    MultiGeoValues.GeoShapeValue triangleTreeValue;
    Extent extentQuery;
    CellIdSource.GeoShapeCellValues cellValues;

    @Setup
    public void setUp() throws Exception {
        XContentParser fijiParser = XContentHelper.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            new BytesArray(
                "{\n" +
                "  \"type\": \"Polygon\",\n" +
                "  \"coordinates\": [\n" +
                "   [\n" +
                "    [\n" +
                "     9.594226,\n" +
                "     47.525058\n" +
                "    ],\n" +
                "    [\n" +
                "     9.632932,\n" +
                "     47.347601\n" +
                "    ],\n" +
                "    [\n" +
                "     9.47997,\n" +
                "     47.10281\n" +
                "    ],\n" +
                "    [\n" +
                "     9.932448,\n" +
                "     46.920728\n" +
                "    ],\n" +
                "    [\n" +
                "     10.442701,\n" +
                "     46.893546\n" +
                "    ],\n" +
                "    [\n" +
                "     10.363378,\n" +
                "     46.483571\n" +
                "    ],\n" +
                "    [\n" +
                "     9.922837,\n" +
                "     46.314899\n" +
                "    ],\n" +
                "    [\n" +
                "     9.182882,\n" +
                "     46.440215\n" +
                "    ],\n" +
                "    [\n" +
                "     8.966306,\n" +
                "     46.036932\n" +
                "    ],\n" +
                "    [\n" +
                "     8.489952,\n" +
                "     46.005151\n" +
                "    ],\n" +
                "    [\n" +
                "     8.31663,\n" +
                "     46.163642\n" +
                "    ],\n" +
                "    [\n" +
                "     7.755992,\n" +
                "     45.82449\n" +
                "    ],\n" +
                "    [\n" +
                "     7.273851,\n" +
                "     45.776948\n" +
                "    ],\n" +
                "    [\n" +
                "     6.843593,\n" +
                "     45.991147\n" +
                "    ],\n" +
                "    [\n" +
                "     6.5001,\n" +
                "     46.429673\n" +
                "    ],\n" +
                "    [\n" +
                "     6.022609,\n" +
                "     46.27299\n" +
                "    ],\n" +
                "    [\n" +
                "     6.037389,\n" +
                "     46.725779\n" +
                "    ],\n" +
                "    [\n" +
                "     6.768714,\n" +
                "     47.287708\n" +
                "    ],\n" +
                "    [\n" +
                "     6.736571,\n" +
                "     47.541801\n" +
                "    ],\n" +
                "    [\n" +
                "     7.192202,\n" +
                "     47.449766\n" +
                "    ],\n" +
                "    [\n" +
                "     7.466759,\n" +
                "     47.620582\n" +
                "    ],\n" +
                "    [\n" +
                "     8.317301,\n" +
                "     47.61358\n" +
                "    ],\n" +
                "    [\n" +
                "     8.522612,\n" +
                "     47.830828\n" +
                "    ],\n" +
                "    [\n" +
                "     9.594226,\n" +
                "     47.525058\n" +
                "    ]\n" +
                "   ]\n" +
                "  ]\n" +
                " }"
            ), XContentType.JSON);

        fijiParser.nextToken();
        Geometry fiji = new GeometryParser(true, true, true).parse(fijiParser);
        geometry = new GeoShapeIndexer(true, "indexer").prepareForIndexing(fiji);

        precision = 9;
        GeoShapeIndexer indexer = new GeoShapeIndexer(true, "test");
        geometry = indexer.prepareForIndexing(geometry);

        GeoShapeCoordinateEncoder encoder = GeoShapeCoordinateEncoder.INSTANCE;
        geometryTreeReader = geometryTreeReader(geometry, encoder);
        triangleTreeReader = triangleTreeReader(geometry, encoder);
        geometryTreeValue = new MultiGeoValues.GeoShapeValue(geometryTreeReader);
        triangleTreeValue = new MultiGeoValues.GeoShapeValue(triangleTreeReader);
        extentQuery = Extent.fromPoints(encoder.encodeX(7.520740), encoder.encodeY(46.202784),
            encoder.encodeX(9.212635), encoder.encodeY(47.539589));

        cellValues = new CellIdSource.GeoShapeCellValues(null, precision, GeoGridTiler.GeoTileGridTiler.INSTANCE);
    }


//
//    @Benchmark
//    public void measureWriting_GeometryTree() throws Exception {
//        GeometryTreeWriter writer = new GeometryTreeWriter(geometry, GeoShapeCoordinateEncoder.INSTANCE);
//        BytesStreamOutput output = new BytesStreamOutput();
//        writer.writeTo(output);
//        output.close();
//    }
//
//    @Benchmark
//    public void measureWriting_TriangleTree() throws Exception {
//        GeometryTreeWriter writer = new GeometryTreeWriter(geometry, GeoShapeCoordinateEncoder.INSTANCE);
//        BytesStreamOutput output = new BytesStreamOutput();
//        writer.writeTo(output);
//        output.close();
//    }

//    @Benchmark
//    public GeoRelation measureRelate_GeometryTree() throws Exception {
//        return geometryTreeReader.relate(extentQuery.minX(), extentQuery.minY(), extentQuery.maxX(), extentQuery.maxY());
//    }
//
//    @Benchmark
//    public GeoRelation measureRelate_TriangleTree() throws Exception {
//        return triangleTreeReader.relate(extentQuery.minX(), extentQuery.minY(), extentQuery.maxX(), extentQuery.maxY());
//    }
//
    @Benchmark
    public int measureGeoTile_GeometryTree() {
        return GeoGridTiler.GeoTileGridTiler.INSTANCE.setValues(cellValues, geometryTreeValue, precision);
    }
//
//
//
//    @Benchmark
//    public int measureGeoTile_TriangleTree() {
//        return GeoGridTiler.GeoTileGridTiler.INSTANCE.setValues(cellValues, triangleTreeValue, precision);
//    }
//
//    @Benchmark
//    public MultiGeoValues.BoundingBox measureBoundingBox_TriangleTree() {
//        return triangleTreeValue.boundingBox();
//    }
//
//    @Benchmark
//    public MultiGeoValues.BoundingBox measureBoundingBox_GeometryTree() {
//        return geometryTreeValue.boundingBox();
//    }

    public static GeometryTreeReader geometryTreeReader(Geometry geometry, CoordinateEncoder encoder) throws IOException {
        GeometryTreeWriter writer = new GeometryTreeWriter(geometry, encoder);
        BytesStreamOutput output = new BytesStreamOutput();
        writer.writeTo(output);
        output.close();
        GeometryTreeReader reader = new GeometryTreeReader(encoder);
        reader.reset(output.bytes().toBytesRef());
        return reader;
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

    public static String toGeoJsonString(Geometry geometry) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        GeoJson.toXContent(geometry, builder, ToXContent.EMPTY_PARAMS);
        return XContentHelper.convertToJson(BytesReference.bytes(builder), true, false, XContentType.JSON);
    }


//    @Benchmark public void test_0_0_1_geometry() throws Exception { geometryTreeReader.relate(-2147483648,0,0,2029398981);}
//    @Benchmark public void test_0_0_1_triangle() throws Exception { geometryTreeReader.relate(-2147483648,0,0,2029398981);}
//    @Benchmark public void test_0_1_1_geometry() throws Exception { geometryTreeReader.relate(-2147483648,-2029398982,0,0);}
//    @Benchmark public void test_0_1_1_triangle() throws Exception { geometryTreeReader.relate(-2147483648,-2029398982,0,0);}
//    @Benchmark public void test_1_0_1_geometry() throws Exception { geometryTreeReader.relate(0,0,2147483647,2029398981);}
//    @Benchmark public void test_1_0_1_triangle() throws Exception { geometryTreeReader.relate(0,0,2147483647,2029398981);}
//    @Benchmark public void test_2_0_2_geometry() throws Exception { geometryTreeReader.relate(0,1587068213,1073741824,2029398981);}
//    @Benchmark public void test_2_0_2_triangle() throws Exception { geometryTreeReader.relate(0,1587068213,1073741824,2029398981);}
//    @Benchmark public void test_2_1_2_geometry() throws Exception { geometryTreeReader.relate(0,0,1073741824,1587068213);}
//    @Benchmark public void test_2_1_2_triangle() throws Exception { geometryTreeReader.relate(0,0,1073741824,1587068213);}
//    @Benchmark public void test_4_2_3_geometry() throws Exception { geometryTreeReader.relate(0,977818455,536870912,1587068213);}
//    @Benchmark public void test_4_2_3_triangle() throws Exception { geometryTreeReader.relate(0,977818455,536870912,1587068213);}
//    @Benchmark public void test_8_4_4_geometry() throws Exception { geometryTreeReader.relate(0,1330880872,268435456,1587068213);}
//    @Benchmark public void test_8_4_4_triangle() throws Exception { geometryTreeReader.relate(0,1330880872,268435456,1587068213);}
//    @Benchmark public void test_8_5_4_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(0,977818455,268435456,1330880872));}
//    @Benchmark public void test_8_5_4_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(0,977818455,268435456,1330880872));}
//    @Benchmark public void test_16_10_5_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(0,1167336302,134217728,1330880872));}
//    @Benchmark public void test_16_10_5_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(0,1167336302,134217728,1330880872));}
//    @Benchmark public void test_16_11_5_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(0,977818455,134217728,1167336302));}
//    @Benchmark public void test_16_11_5_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(0,977818455,134217728,1167336302));}
//    @Benchmark public void test_32_22_6_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(0,1075866295,67108864,1167336302));}
//    @Benchmark public void test_32_22_6_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(0,1075866295,67108864,1167336302));}
//    @Benchmark public void test_32_23_6_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(0,977818455,67108864,1075866295));}
//    @Benchmark public void test_32_23_6_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(0,977818455,67108864,1075866295));}
//    @Benchmark public void test_33_22_6_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(67108864,1075866295,134217728,1167336302));}
//    @Benchmark public void test_33_22_6_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(67108864,1075866295,134217728,1167336302));}
//    @Benchmark public void test_66_44_7_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(67108864,1122422466,100663296,1167336302));}
//    @Benchmark public void test_66_44_7_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(67108864,1122422466,100663296,1167336302));}
//    @Benchmark public void test_132_88_8_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(67108864,1145084133,83886080,1167336302));}
//    @Benchmark public void test_132_88_8_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(67108864,1145084133,83886080,1167336302));}
//    @Benchmark public void test_132_89_8_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(67108864,1122422466,83886080,1145084133));}
//    @Benchmark public void test_132_89_8_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(67108864,1122422466,83886080,1145084133));}
//    @Benchmark public void test_264_178_9_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(67108864,1133804572,75497472,1145084133));}
//    @Benchmark public void test_264_178_9_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(67108864,1133804572,75497472,1145084133));}
//    @Benchmark public void test_264_179_9_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(67108864,1122422466,75497472,1133804572));}
//    @Benchmark public void test_264_179_9_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(67108864,1122422466,75497472,1133804572));}
//    @Benchmark public void test_265_178_9_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(75497472,1133804572,83886080,1145084133));}
//    @Benchmark public void test_265_178_9_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(75497472,1133804572,83886080,1145084133));}
//    @Benchmark public void test_265_179_9_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(75497472,1122422466,83886080,1133804572));}
//    @Benchmark public void test_265_179_9_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(75497472,1122422466,83886080,1133804572));}
//    @Benchmark public void test_133_88_8_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(83886080,1145084133,100663296,1167336302));}
//    @Benchmark public void test_133_88_8_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(83886080,1145084133,100663296,1167336302));}
//    @Benchmark public void test_133_89_8_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(83886080,1122422466,100663296,1145084133));}
//    @Benchmark public void test_133_89_8_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(83886080,1122422466,100663296,1145084133));}
//    @Benchmark public void test_266_178_9_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(83886080,1133804572,92274688,1145084133));}
//    @Benchmark public void test_266_178_9_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(83886080,1133804572,92274688,1145084133));}
//    @Benchmark public void test_266_179_9_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(83886080,1122422466,92274688,1133804572));}
//    @Benchmark public void test_266_179_9_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(83886080,1122422466,92274688,1133804572));}
//    @Benchmark public void test_267_178_9_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(92274688,1133804572,100663296,1145084133));}
//    @Benchmark public void test_267_178_9_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(92274688,1133804572,100663296,1145084133));}
//    @Benchmark public void test_267_179_9_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(92274688,1122422466,100663296,1133804572));}
//    @Benchmark public void test_267_179_9_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(92274688,1122422466,100663296,1133804572));}
//    @Benchmark public void test_66_45_7_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(67108864,1075866295,100663296,1122422466));}
//    @Benchmark public void test_66_45_7_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(67108864,1075866295,100663296,1122422466));}
//    @Benchmark public void test_132_90_8_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(67108864,1099350104,83886080,1122422466));}
//    @Benchmark public void test_132_90_8_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(67108864,1099350104,83886080,1122422466));}
//    @Benchmark public void test_264_180_9_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(67108864,1110937679,75497472,1122422466));}
//    @Benchmark public void test_264_180_9_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(67108864,1110937679,75497472,1122422466));}
//    @Benchmark public void test_264_181_9_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(67108864,1099350104,75497472,1110937679));}
//    @Benchmark public void test_264_181_9_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(67108864,1099350104,75497472,1110937679));}
//    @Benchmark public void test_265_180_9_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(75497472,1110937679,83886080,1122422466));}
//    @Benchmark public void test_265_180_9_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(75497472,1110937679,83886080,1122422466));}
//    @Benchmark public void test_265_181_9_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(75497472,1099350104,83886080,1110937679));}
//    @Benchmark public void test_265_181_9_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(75497472,1099350104,83886080,1110937679));}
//    @Benchmark public void test_132_91_8_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(67108864,1075866295,83886080,1099350104));}
//    @Benchmark public void test_132_91_8_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(67108864,1075866295,83886080,1099350104));}
//    @Benchmark public void test_264_182_9_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(67108864,1087659659,75497472,1099350104));}
//    @Benchmark public void test_264_182_9_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(67108864,1087659659,75497472,1099350104));}
//    @Benchmark public void test_264_183_9_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(67108864,1075866295,75497472,1087659659));}
//    @Benchmark public void test_264_183_9_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(67108864,1075866295,75497472,1087659659));}
//    @Benchmark public void test_265_182_9_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(75497472,1087659659,83886080,1099350104));}
//    @Benchmark public void test_265_182_9_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(75497472,1087659659,83886080,1099350104));}
//    @Benchmark public void test_265_183_9_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(75497472,1075866295,83886080,1087659659));}
//    @Benchmark public void test_265_183_9_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(75497472,1075866295,83886080,1087659659));}
//    @Benchmark public void test_133_90_8_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(83886080,1099350104,100663296,1122422466));}
//    @Benchmark public void test_133_90_8_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(83886080,1099350104,100663296,1122422466));}
//    @Benchmark public void test_266_180_9_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(83886080,1110937679,92274688,1122422466));}
//    @Benchmark public void test_266_180_9_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(83886080,1110937679,92274688,1122422466));}
//    @Benchmark public void test_266_181_9_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(83886080,1099350104,92274688,1110937679));}
//    @Benchmark public void test_266_181_9_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(83886080,1099350104,92274688,1110937679));}
//    @Benchmark public void test_267_180_9_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(92274688,1110937679,100663296,1122422466));}
//    @Benchmark public void test_267_180_9_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(92274688,1110937679,100663296,1122422466));}
//    @Benchmark public void test_267_181_9_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(92274688,1099350104,100663296,1110937679));}
//    @Benchmark public void test_267_181_9_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(92274688,1099350104,100663296,1110937679));}
//    @Benchmark public void test_133_91_8_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(83886080,1075866295,100663296,1099350104));}
//    @Benchmark public void test_133_91_8_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(83886080,1075866295,100663296,1099350104));}
//    @Benchmark public void test_266_182_9_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(83886080,1087659659,92274688,1099350104));}
//    @Benchmark public void test_266_182_9_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(83886080,1087659659,92274688,1099350104));}
//    @Benchmark public void test_266_183_9_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(83886080,1075866295,92274688,1087659659));}
//    @Benchmark public void test_266_183_9_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(83886080,1075866295,92274688,1087659659));}
//    @Benchmark public void test_267_182_9_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(92274688,1087659659,100663296,1099350104));}
//    @Benchmark public void test_267_182_9_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(92274688,1087659659,100663296,1099350104));}
//    @Benchmark public void test_267_183_9_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(92274688,1075866295,100663296,1087659659));}
//    @Benchmark public void test_267_183_9_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(92274688,1075866295,100663296,1087659659));}
//    @Benchmark public void test_67_44_7_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(100663296,1122422466,134217728,1167336302));}
//    @Benchmark public void test_67_44_7_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(100663296,1122422466,134217728,1167336302));}
//    @Benchmark public void test_134_88_8_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(100663296,1145084133,117440512,1167336302));}
//    @Benchmark public void test_134_88_8_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(100663296,1145084133,117440512,1167336302));}
//    @Benchmark public void test_134_89_8_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(100663296,1122422466,117440512,1145084133));}
//    @Benchmark public void test_134_89_8_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(100663296,1122422466,117440512,1145084133));}
//    @Benchmark public void test_268_178_9_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(100663296,1133804572,109051904,1145084133));}
//    @Benchmark public void test_268_178_9_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(100663296,1133804572,109051904,1145084133));}
//    @Benchmark public void test_268_179_9_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(100663296,1122422466,109051904,1133804572));}
//    @Benchmark public void test_268_179_9_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(100663296,1122422466,109051904,1133804572));}
//    @Benchmark public void test_269_178_9_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(109051904,1133804572,117440512,1145084133));}
//    @Benchmark public void test_269_178_9_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(109051904,1133804572,117440512,1145084133));}
//    @Benchmark public void test_269_179_9_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(109051904,1122422466,117440512,1133804572));}
//    @Benchmark public void test_269_179_9_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(109051904,1122422466,117440512,1133804572));}
//    @Benchmark public void test_135_88_8_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(117440512,1145084133,134217728,1167336302));}
//    @Benchmark public void test_135_88_8_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(117440512,1145084133,134217728,1167336302));}
//    @Benchmark public void test_135_89_8_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(117440512,1122422466,134217728,1145084133));}
//    @Benchmark public void test_135_89_8_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(117440512,1122422466,134217728,1145084133));}
//    @Benchmark public void test_67_45_7_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(100663296,1075866295,134217728,1122422466));}
//    @Benchmark public void test_67_45_7_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(100663296,1075866295,134217728,1122422466));}
//    @Benchmark public void test_134_90_8_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(100663296,1099350104,117440512,1122422466));}
//    @Benchmark public void test_134_90_8_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(100663296,1099350104,117440512,1122422466));}
//    @Benchmark public void test_268_180_9_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(100663296,1110937679,109051904,1122422466));}
//    @Benchmark public void test_268_180_9_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(100663296,1110937679,109051904,1122422466));}
//    @Benchmark public void test_268_181_9_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(100663296,1099350104,109051904,1110937679));}
//    @Benchmark public void test_268_181_9_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(100663296,1099350104,109051904,1110937679));}
//    @Benchmark public void test_269_180_9_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(109051904,1110937679,117440512,1122422466));}
//    @Benchmark public void test_269_180_9_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(109051904,1110937679,117440512,1122422466));}
//    @Benchmark public void test_269_181_9_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(109051904,1099350104,117440512,1110937679));}
//    @Benchmark public void test_269_181_9_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(109051904,1099350104,117440512,1110937679));}
//    @Benchmark public void test_134_91_8_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(100663296,1075866295,117440512,1099350104));}
//    @Benchmark public void test_134_91_8_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(100663296,1075866295,117440512,1099350104));}
//    @Benchmark public void test_268_182_9_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(100663296,1087659659,109051904,1099350104));}
//    @Benchmark public void test_268_182_9_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(100663296,1087659659,109051904,1099350104));}
//    @Benchmark public void test_268_183_9_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(100663296,1075866295,109051904,1087659659));}
//    @Benchmark public void test_268_183_9_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(100663296,1075866295,109051904,1087659659));}
//    @Benchmark public void test_269_182_9_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(109051904,1087659659,117440512,1099350104));}
//    @Benchmark public void test_269_182_9_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(109051904,1087659659,117440512,1099350104));}
//    @Benchmark public void test_269_183_9_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(109051904,1075866295,117440512,1087659659));}
//    @Benchmark public void test_269_183_9_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(109051904,1075866295,117440512,1087659659));}
//    @Benchmark public void test_135_90_8_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(117440512,1099350104,134217728,1122422466));}
//    @Benchmark public void test_135_90_8_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(117440512,1099350104,134217728,1122422466));}
//    @Benchmark public void test_270_180_9_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(117440512,1110937679,125829120,1122422466));}
//    @Benchmark public void test_270_180_9_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(117440512,1110937679,125829120,1122422466));}
//    @Benchmark public void test_270_181_9_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(117440512,1099350104,125829120,1110937679));}
//    @Benchmark public void test_270_181_9_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(117440512,1099350104,125829120,1110937679));}
//    @Benchmark public void test_271_180_9_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(125829120,1110937679,134217728,1122422466));}
//    @Benchmark public void test_271_180_9_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(125829120,1110937679,134217728,1122422466));}
//    @Benchmark public void test_271_181_9_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(125829120,1099350104,134217728,1110937679));}
//    @Benchmark public void test_271_181_9_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(125829120,1099350104,134217728,1110937679));}
//    @Benchmark public void test_135_91_8_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(117440512,1075866295,134217728,1099350104));}
//    @Benchmark public void test_135_91_8_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(117440512,1075866295,134217728,1099350104));}
//    @Benchmark public void test_33_23_6_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(67108864,977818455,134217728,1075866295));}
//    @Benchmark public void test_33_23_6_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(67108864,977818455,134217728,1075866295));}
//    @Benchmark public void test_17_10_5_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(134217728,1167336302,268435456,1330880872));}
//    @Benchmark public void test_17_10_5_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(134217728,1167336302,268435456,1330880872));}
//    @Benchmark public void test_17_11_5_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(134217728,977818455,268435456,1167336302));}
//    @Benchmark public void test_17_11_5_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(134217728,977818455,268435456,1167336302));}
//    @Benchmark public void test_9_4_4_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(268435456,1330880872,536870912,1587068213));}
//    @Benchmark public void test_9_4_4_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(268435456,1330880872,536870912,1587068213));}
//    @Benchmark public void test_9_5_4_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(268435456,977818455,536870912,1330880872));}
//    @Benchmark public void test_9_5_4_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(268435456,977818455,536870912,1330880872));}
//    @Benchmark public void test_4_3_3_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(0,0,536870912,977818455));}
//    @Benchmark public void test_4_3_3_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(0,0,536870912,977818455));}
//    @Benchmark public void test_5_2_3_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(536870912,977818455,1073741824,1587068213));}
//    @Benchmark public void test_5_2_3_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(536870912,977818455,1073741824,1587068213));}
//    @Benchmark public void test_5_3_3_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(536870912,0,1073741824,977818455));}
//    @Benchmark public void test_5_3_3_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(536870912,0,1073741824,977818455));}
//    @Benchmark public void test_3_0_2_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(1073741824,1587068213,2147483647,2029398981));}
//    @Benchmark public void test_3_0_2_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(1073741824,1587068213,2147483647,2029398981));}
//    @Benchmark public void test_3_1_2_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(1073741824,0,2147483647,1587068213));}
//    @Benchmark public void test_3_1_2_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(1073741824,0,2147483647,1587068213));}
//    @Benchmark public void test_1_1_1_geometry() throws Exception { geometryTreeReader.relate(Extent.fromPoints(0,-2029398982,2147483647,0));}
//    @Benchmark public void test_1_1_1_triangle() throws Exception { geometryTreeReader.relate(Extent.fromPoints(0,-2029398982,2147483647,0));}


}
