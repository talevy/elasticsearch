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
package org.elasticsearch.plugin.wordcount;

import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.plain.DocValuesIndexFieldData;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.SimpleMappedFieldType;
import org.elasticsearch.index.mapper.TypeParsers;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.mapper.NumberFieldMapper.NumberType.INTEGER;

public class WordCountFieldMapper extends FieldMapper {

    static final String CONTENT_TYPE = "wordcount";

    public static class Defaults {
        public static final MappedFieldType FIELD_TYPE = new WordCountFieldType();

        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setIndexOptions(IndexOptions.NONE);
            FIELD_TYPE.setHasDocValues(true);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.freeze();
        }
    }

    WordCountFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType, Settings indexSettings, MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, indexSettings, multiFields, copyTo);
    }

    @Override
    protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {
        String value = context.parser().text();
        Integer wordcount = value.split(" ").length;
        fields.addAll(INTEGER.createFields(fieldType().name(), wordcount, false, true, true));
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    public static final class WordCountFieldType extends SimpleMappedFieldType {

        public WordCountFieldType() {
            super();
            setHasDocValues(true);
        }

        @Override
        public MappedFieldType clone() {
            return new WordCountFieldType();
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            return null;
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            return null;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName) {
            failIfNoDocValues();
            return new DocValuesIndexFieldData.Builder().numericType(IndexNumericFieldData.NumericType.INT);
        }
    }

    public static class Builder extends FieldMapper.Builder<Builder, WordCountFieldMapper> {
        Builder(String name) {
            super(name, Defaults.FIELD_TYPE, Defaults.FIELD_TYPE);
        }

        @Override
        public WordCountFieldMapper build(BuilderContext context) {
            setupFieldType(context);
            return new WordCountFieldMapper(name, fieldType, defaultFieldType, context.indexSettings(),
                multiFieldsBuilder.build(this, context), copyTo);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {

        @Override
        public Mapper.Builder<?, ?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            WordCountFieldMapper.Builder builder = new WordCountFieldMapper.Builder(name);
            TypeParsers.parseField(builder, name, node, parserContext);
            return builder;
        }
    }
}
