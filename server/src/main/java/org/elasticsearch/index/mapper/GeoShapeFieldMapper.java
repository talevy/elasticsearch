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

import org.apache.lucene.document.Field;
import org.apache.lucene.document.LatLonShape;
import org.apache.lucene.geo.Line;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.geo.parsers.ShapeParser;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.geo.geometry.Circle;
import org.elasticsearch.geo.geometry.Geometry;
import org.elasticsearch.geo.geometry.GeometryCollection;
import org.elasticsearch.geo.geometry.GeometryVisitor;
import org.elasticsearch.geo.geometry.LinearRing;
import org.elasticsearch.geo.geometry.MultiLine;
import org.elasticsearch.geo.geometry.MultiPoint;
import org.elasticsearch.geo.geometry.MultiPolygon;
import org.elasticsearch.geo.geometry.Point;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.plain.BinaryDVIndexFieldData;
import org.elasticsearch.index.fielddata.plain.DocValuesIndexFieldData;
import org.elasticsearch.indices.breaker.CircuitBreakerService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * FieldMapper for indexing {@link LatLonShape}s.
 * <p>
 * Currently Shapes can only be indexed and can only be queried using
 * {@link org.elasticsearch.index.query.GeoShapeQueryBuilder}, consequently
 * a lot of behavior in this Mapper is disabled.
 * <p>
 * Format supported:
 * <p>
 * "field" : {
 * "type" : "polygon",
 * "coordinates" : [
 * [ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0] ]
 * ]
 * }
 * <p>
 * or:
 * <p>
 * "field" : "POLYGON ((100.0 0.0, 101.0 0.0, 101.0 1.0, 100.0 1.0, 100.0 0.0))
 */
public class GeoShapeFieldMapper extends BaseGeoShapeFieldMapper {

    public static class Builder extends BaseGeoShapeFieldMapper.Builder<BaseGeoShapeFieldMapper.Builder, GeoShapeFieldMapper> {
        public Builder(String name) {
            super (name, new GeoShapeFieldType(), new GeoShapeFieldType());
        }

        @Override
        public GeoShapeFieldMapper build(BuilderContext context) {
            setupFieldType(context);
            // TODO(talevy)  context.indexCreatedVersion() to fetch version
            return new GeoShapeFieldMapper(name, fieldType, defaultFieldType, ignoreMalformed(context), coerce(context),
                ignoreZValue(), context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo);
        }
    }

    public static final class GeoShapeFieldType extends BaseGeoShapeFieldType {
        public GeoShapeFieldType() {
            super();
            setHasDocValues(true); // TODO(talevy): version guard this version.onOrAfter(Version.CURRENT)
        }


        protected GeoShapeFieldType(GeoShapeFieldType ref) {
            super(ref);
        }

        @Override
        public GeoShapeFieldType clone() {
            return new GeoShapeFieldType(this);
        }

//        @Override
//        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName) {
//            failIfNoDocValues();
//            return new DocValuesIndexFieldData.Builder();
//        }
        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName) {
            return new IndexFieldData.Builder() {
                @Override
                public IndexFieldData<?> build(
                    IndexSettings indexSettings, MappedFieldType fieldType, IndexFieldDataCache cache, CircuitBreakerService breakerService, MapperService mapperService) {
                    return new BinaryDVIndexFieldData(indexSettings.getIndex(), fieldType.name());
                }
            };
        }
    }

    public GeoShapeFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                               Explicit<Boolean> ignoreMalformed, Explicit<Boolean> coerce,
                               Explicit<Boolean> ignoreZValue, Settings indexSettings,
                               MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, ignoreMalformed, coerce, ignoreZValue, indexSettings,
            multiFields, copyTo);
    }

    @Override
    public GeoShapeFieldType fieldType() {
        return (GeoShapeFieldType) super.fieldType();
    }

    /** parsing logic for {@link LatLonShape} indexing */
    @Override
    public void parse(ParseContext context) throws IOException {
        try {
            Geometry shape = context.parseExternalValue(Geometry.class);
            if (shape == null) {
                ShapeBuilder shapeBuilder = ShapeParser.parse(context.parser(), this);
                if (shapeBuilder == null) {
                    return;
                }
                shape = shapeBuilder.buildGeometry();
            }
            indexShape(context, shape);
            // TODO(talevy): currently assumes all shapes are polygons
            if (fieldType().hasDocValues()) {
                String name = fieldType().name();
                BinaryPolygon2DDocValuesField docValuesField = (BinaryPolygon2DDocValuesField) context.doc().getByKey(name);
                if (docValuesField == null) {
                    docValuesField = new BinaryPolygon2DDocValuesField(name, shape);
                    context.doc().addWithKey(name, docValuesField);
                } else {
                    docValuesField.add(shape);
                }
            }
        } catch (Exception e) {
            if (ignoreMalformed.value() == false) {
                throw new MapperParsingException("failed to parse field [{}] of type [{}]", e, fieldType().name(),
                    fieldType().typeName());
            }
            context.addIgnoredField(fieldType().name());
        }
    }

    private void indexShape(ParseContext context, Object luceneShape) {
        if (luceneShape instanceof Geometry) {
            ((Geometry) luceneShape).visit(new LuceneGeometryIndexer(context));
        } else {
            throw new IllegalArgumentException("invalid shape type found [" + luceneShape.getClass() + "] while indexing shape");
        }
    }

    private class LuceneGeometryIndexer implements GeometryVisitor<Void> {
        private ParseContext context;

        private LuceneGeometryIndexer(ParseContext context) {
            this.context = context;
        }

        @Override
        public Void visit(Circle circle) {
            throw new IllegalArgumentException("invalid shape type found [Circle] while indexing shape");
        }

        @Override
        public Void visit(GeometryCollection<?> collection) {
            for (Geometry geometry : collection) {
                geometry.visit(this);
            }
            return null;
        }

        @Override
        public Void visit(org.elasticsearch.geo.geometry.Line line) {
            indexFields(context, LatLonShape.createIndexableFields(name(), new Line(line.getLats(), line.getLons())));
            return null;
        }

        @Override
        public Void visit(LinearRing ring) {
            throw new IllegalArgumentException("invalid shape type found [LinearRing] while indexing shape");
        }

        @Override
        public Void visit(MultiLine multiLine) {
            for (org.elasticsearch.geo.geometry.Line line : multiLine) {
                visit(line);
            }
            return null;
        }

        @Override
        public Void visit(MultiPoint multiPoint) {
            for(Point point : multiPoint) {
                visit(point);
            }
            return null;
        }

        @Override
        public Void visit(MultiPolygon multiPolygon) {
            for(org.elasticsearch.geo.geometry.Polygon polygon : multiPolygon) {
                visit(polygon);
            }
            return null;
        }

        @Override
        public Void visit(Point point) {
            indexFields(context, LatLonShape.createIndexableFields(name(), point.getLat(), point.getLon()));
            return null;
        }

        @Override
        public Void visit(org.elasticsearch.geo.geometry.Polygon polygon) {
            indexFields(context, LatLonShape.createIndexableFields(name(), toLucenePolygon(polygon)));
            return null;
        }

        @Override
        public Void visit(org.elasticsearch.geo.geometry.Rectangle r) {
            Polygon p = new Polygon(new double[]{r.getMinLat(), r.getMinLat(), r.getMaxLat(), r.getMaxLat(), r.getMinLat()},
                new double[]{r.getMinLon(), r.getMaxLon(), r.getMaxLon(), r.getMinLon(), r.getMinLon()});
            indexFields(context, LatLonShape.createIndexableFields(name(), p));
            return null;
        }
    }

    public static Polygon toLucenePolygon(org.elasticsearch.geo.geometry.Polygon polygon) {
        Polygon[] holes = new Polygon[polygon.getNumberOfHoles()];
        for(int i = 0; i<holes.length; i++) {
            holes[i] = new Polygon(polygon.getHole(i).getLats(), polygon.getHole(i).getLons());
        }
        return new Polygon(polygon.getPolygon().getLats(), polygon.getPolygon().getLons(), holes);
    }

    private void indexFields(ParseContext context, Field[] fields) {
        ArrayList<IndexableField> flist = new ArrayList<>(Arrays.asList(fields));
        createFieldNamesField(context, flist);
        for (IndexableField f : flist) {
            context.doc().add(f);
        }
    }
}
