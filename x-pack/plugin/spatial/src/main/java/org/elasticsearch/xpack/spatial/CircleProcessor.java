/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.spatial;

import org.apache.lucene.geo.GeoUtils;
import org.apache.lucene.util.SloppyMath;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.geo.GeometryFormat;
import org.elasticsearch.common.geo.GeometryParser;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.MapXContentParser;
import org.elasticsearch.geo.geometry.Circle;
import org.elasticsearch.geo.geometry.Geometry;
import org.elasticsearch.geo.geometry.LinearRing;
import org.elasticsearch.geo.geometry.Polygon;
import org.elasticsearch.geo.geometry.ShapeType;
import org.elasticsearch.geo.utils.GeographyValidator;
import org.elasticsearch.geo.utils.WellKnownText;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;

import java.io.IOException;
import java.text.ParseException;
import java.util.Collections;
import java.util.Map;


/**
 *  The circle-processor converts a circle shape definition into a valid geojson-polygon approximating the circle.
 */
public final class CircleProcessor extends AbstractProcessor {
    public static final String TYPE = "circle";
    static final WellKnownText WKT = new WellKnownText(true, new GeographyValidator(true));
    private final GeometryParser GEOMETRY_PARSER = new GeometryParser(true, true, true);
    private final String PARSE_FIELD = "parsingField";


    private final String field;
    private final String targetField;
    private final double errorInMeters;
    private final int maxSides;
    private final boolean ignoreMissing;


    CircleProcessor(String tag, String field, String targetField, boolean ignoreMissing, double errorInMeters, int maxSides) {
        super(tag);
        this.field = field;
        this.targetField = targetField;
        this.ignoreMissing = ignoreMissing;
        this.errorInMeters = errorInMeters;
        this.maxSides = maxSides;
    }

    /**
     *  This method returns an N-gon {@link Polygon} that approximates a circle. N is determined
     *  by an approximation function of <code>errorInMeters</code>. N is at most <code>maxSides</code>.
     *
     * @param centerLat     the center latitude coordinate of the circle
     * @param centerLon     the center longitude coordinate of the circle
     * @param radiusMeters  the radius of the circle to approximate
     *
     * @return a {@link Polygon} approximating the input circle.
     */
    Polygon polygonize(double centerLat, double centerLon, double radiusMeters) {
        double thetaInDegrees = Math.acos(Math.toRadians(2 * Math.pow(2, 1 - errorInMeters / radiusMeters) - 1));
        int numberOfSides = Math.min((int) Math.ceil(360 / thetaInDegrees), maxSides);
        return createRegularPolygon(centerLat, centerLon, radiusMeters, numberOfSides);
    }


    @Override
    @SuppressWarnings("unchecked")
    public IngestDocument execute(IngestDocument ingestDocument) {
        Object obj = ingestDocument.getFieldValue(field, Object.class, ignoreMissing);

        if (obj == null && ignoreMissing) {
            return ingestDocument;
        } else if (obj == null) {
            throw new IllegalArgumentException("field [" + field + "] is null, cannot extract circle.");
        }

        Map<String, Object> map = Collections.singletonMap(PARSE_FIELD, obj);
        final XContentParser parser = new MapXContentParser(NamedXContentRegistry.EMPTY,
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION, map, XContentType.JSON);
        try {
            parser.nextToken(); // START_OBJECT
            parser.nextToken(); // PARSE_FIELD field
            parser.nextToken(); // PARSE_FIELD value
            GeometryFormat geoFormat = GEOMETRY_PARSER.geometryFormat(parser);
            Geometry geometry = geoFormat.fromXContent(parser);
            if (ShapeType.CIRCLE.equals(geometry.type())) {
                Circle circle = (Circle) geometry;
                Polygon poly = polygonize(circle.getLat(), circle.getLon(), circle.getRadiusMeters());
                XContentBuilder builder = XContentFactory.jsonBuilder();
                builder.startObject();
                builder.field(PARSE_FIELD);
                geoFormat.toXContent(poly, builder, ToXContent.EMPTY_PARAMS);
                builder.endObject();
                Tuple<XContentType, Map<String, Object>> xContentTuple = XContentHelper
                    .convertToMap(BytesReference.bytes(builder), true, XContentType.JSON);
                ingestDocument.setFieldValue(targetField, xContentTuple.v2().get(PARSE_FIELD));
            } else {
                throw new IllegalArgumentException("geometry must be a circle. found [" + geometry.type() + "]");
            }
        } catch (IOException | ParseException | XContentParseException e) {
            throw new IllegalArgumentException("invalid geometry [" + obj + "]", e);
        }

        return ingestDocument;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    String getField() {
        return field;
    }

    String getTargetField() {
        return targetField;
    }

    double getErrorInMeters() {
        return errorInMeters;
    }

    int getMaxSides() {
        return maxSides;
    }

    /** Makes an n-gon, centered at the provided lat/lon, and each vertex approximately
     *  radiusMeters away from the center.
     *
     * Do not invoke me across the dateline or a pole!! */
    static Polygon createRegularPolygon(double centerLat, double centerLon, double radiusMeters, int gons) {
        double[][] result = new double[2][];
        result[0] = new double[gons+1];
        result[1] = new double[gons+1];
        for(int i=0;i<gons;i++) {
            double angle = 360.0-i*(360.0/gons);
            double x = Math.cos(SloppyMath.toRadians(angle));
            double y = Math.sin(SloppyMath.toRadians(angle));
            double factor = 2.0;
            double step = 1.0;
            int last = 0;

            // Iterate out along one spoke until we hone in on the point that's nearly exactly radiusMeters from the center:
            while (true) {
                // TODO: we could in fact cross a pole?  Just do what surpriseMePolygon does?
                double lat = centerLat + y * factor;
                GeoUtils.checkLatitude(lat);
                double lon = centerLon + x * factor;
                GeoUtils.checkLongitude(lon);
                double distanceMeters = SloppyMath.haversinMeters(centerLat, centerLon, lat, lon);

                if (Math.abs(distanceMeters - radiusMeters) < 0.1) {
                    // Within 10 cm: close enough!
                    result[0][i] = lat;
                    result[1][i] = lon;
                    break;
                }

                if (distanceMeters > radiusMeters) {
                    // too big
                    factor -= step;
                    if (last == 1) {
                        step /= 2.0;
                    }
                    last = -1;
                } else if (distanceMeters < radiusMeters) {
                    // too small
                    factor += step;
                    if (last == -1) {
                        step /= 2.0;
                    }
                    last = 1;
                }
            }
        }

        // close poly
        result[0][gons] = result[0][0];
        result[1][gons] = result[1][0];

        return new Polygon(new LinearRing(result[0], result[1]));
    }

    public static final class Factory implements Processor.Factory {
        static final double DEFAULT_ERROR = 0.1;
        static final int DEFAULT_MAX_SIDES = 5000;

        public CircleProcessor create(Map<String, Processor.Factory> registry, String processorTag, Map<String, Object> config) {
            String field = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "field");
            String targetField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "target_field", field);
            double errorInMeters = ConfigurationUtils.readDoubleProperty(TYPE, processorTag, config, "error_in_meters", DEFAULT_ERROR);
            int maxPoints = ConfigurationUtils.readIntProperty(TYPE, processorTag, config, "max_sides", DEFAULT_MAX_SIDES);
            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "ignore_missing", false);
            return new CircleProcessor(processorTag, field, targetField, ignoreMissing, errorInMeters, maxPoints);
        }
    }
}
