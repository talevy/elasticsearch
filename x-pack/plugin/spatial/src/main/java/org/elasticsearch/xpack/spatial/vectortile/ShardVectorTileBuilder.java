/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.spatial.vectortile;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.document.LatLonShape;
import org.apache.lucene.document.ShapeField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fieldvisitor.SingleFieldsVisitor;
import org.elasticsearch.index.mapper.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.GeoShapeFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class ShardVectorTileBuilder {
    private static final Logger logger = LogManager.getLogger(ShardVectorTileBuilder.class);

    private final IndexShard indexShard;
    private final Engine.Searcher searcher;
    private final SearchExecutionContext searchExecutionContext;
    private final MappedFieldType geoField;
    private final MappedFieldType sourceField;
    private final int z;
    private final int x;
    private final int y;
    private Object vectorTile; // TODO to be set in #execute

    ShardVectorTileBuilder(IndexService indexService,
                           ShardId shardId,
                           String field,
                           int z,
                           int x,
                           int y) {
        this.indexShard = indexService.getShard(shardId.id());
        this.searcher = indexShard.acquireSearcher("vectortile");
        Closeable toClose = searcher;
        try {
            this.searchExecutionContext = indexService.newSearchExecutionContext(
                indexShard.shardId().id(),
                0,
                searcher,
                () -> 0L,
                null,
                Collections.emptyMap()
            );
            this.geoField = searchExecutionContext.getFieldType(field);
            this.sourceField = searchExecutionContext.getFieldType(SourceFieldMapper.NAME);
            this.z = z;
            this.x = x;
            this.y = y;
            verifyGeoField(geoField);
            toClose = null;
        } finally {
            IOUtils.closeWhileHandlingException(toClose);
        }
    }

    /**
     * To be used by {@link TransportVectorTileAction} to return the vector tile to client
     */
    // TODO
    public Object getVectorTile() {
        return vectorTile;
    }

    private void verifyGeoField(MappedFieldType fieldType) {
        if (fieldType == null) {
            throw new IllegalArgumentException("fieldType is null");
        }
        if (fieldType instanceof GeoPointFieldMapper.GeoPointFieldType == false
                && fieldType instanceof GeoShapeFieldMapper.GeoShapeFieldType == false) {
            throw new IllegalArgumentException("Wrong type for the geo field, " +
                "expected [geo_point,geo_shape], got [" + fieldType.name()  + "]");
        }
        if (fieldType.isSearchable() == false) {
            throw new IllegalArgumentException("The geo field [" + fieldType.name() +  "]  is not searchable");
        }
    }

    public void execute() throws IOException {
        try (searcher) {
            Rectangle rectangle = GeoTileUtils.toBoundingBox(x, y, z);
            Query tileQuery = LatLonShape.newBoxQuery(geoField.name(), ShapeField.QueryRelation.INTERSECTS, rectangle.getMinLat(),
                rectangle.getMaxLat(),rectangle.getMinLon(), rectangle.getMaxLon());
            GeoCollector collector = new GeoCollector();
            searcher.search(tileQuery, collector);
            vectorTile = collector.getVectorTile();
        }
    }


    /**
     * Keeps track and builds Vector Tile object from documents
     */
    private class GeoCollector implements Collector {
        private GeoCollector() {
        }

        /**
         * Returns a representation of the vector tile for this shard's results to be returned
         */
        // TODO
        public Object getVectorTile() {
            return null;
        }

        @Override
        public LeafCollector getLeafCollector(LeafReaderContext context) {
            return new LeafCollector() {

                @Override
                public void setScorer(Scorable scorer) {
                }

                @Override
                public void collect(int docID) throws IOException {
                    List<Object> values = new ArrayList<>();
                    SingleFieldsVisitor visitor = new SingleFieldsVisitor(sourceField, values);
                    context.reader().document(docID, visitor);
                    // TODO: the source must be parsed into a geometry!
                }
            };
        }

        @Override
        public ScoreMode scoreMode() {
            return ScoreMode.COMPLETE_NO_SCORES;
        }
    }
}
