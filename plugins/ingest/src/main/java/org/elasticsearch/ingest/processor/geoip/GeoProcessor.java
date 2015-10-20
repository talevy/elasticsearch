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

package org.elasticsearch.ingest.processor.geoip;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.record.*;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.ingest.Data;
import org.elasticsearch.ingest.processor.Processor;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;

// TODO(talevy): introduce LRU-cache, database hits are suprisingly expensive.
// TODO(talevy): add ability to filter fields retrieved from database
// TODO(talevy): support databases other than "City".
// TODO(talevy): support other sources from the database, other than local filesystem. + figure out security
public final class GeoProcessor implements Processor {

    public static final String TYPE = "geoip";

    private final String ipField;
    private final String databasePath;
    private final String targetField;
    private DatabaseReader dbReader;

    public GeoProcessor(String ipField, String databasePath, String targetField) {
        this.ipField = ipField;
        this.databasePath = databasePath;
        this.targetField = (targetField == null) ? "geoip" : targetField;

        File database = new File(databasePath);
        try {
            this.dbReader = new DatabaseReader.Builder(database).build();
        } catch (IOException e) {
            // TODO(talevy): handle exception
        }
    }

    @Override
    public void execute(Data data) {
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) { sm.checkPermission(new SpecialPermission()); }
        AccessController.doPrivileged(new PrivilegedAction<Void>() {
            @Override
            public Void run() {
                try {
                    String ip = (String) data.getProperty(ipField);
                    InetAddress ipAddress = InetAddress.getByName(ip);

                    CityResponse response = dbReader.city(ipAddress);
                    Country country = response.getCountry();
                    City city = response.getCity();
                    Location location = response.getLocation();
                    Continent continent = response.getContinent();
                    Subdivision subdivision = response.getMostSpecificSubdivision();

                    HashMap<String, Object> geoData = new HashMap<String, Object>();
                    geoData.put("ip", ipAddress.getHostAddress());
                    geoData.put("country_iso_code", country.getIsoCode());
                    geoData.put("country_name", country.getName());
                    geoData.put("continent_name", continent.getName());
                    geoData.put("region_name", subdivision.getName());
                    geoData.put("city_name", city.getName());
                    geoData.put("timezone", location.getTimeZone());
                    geoData.put("latitude", location.getLatitude());
                    geoData.put("longitude", location.getLongitude());
                    geoData.put("location", new double[] { location.getLongitude(), location.getLatitude() });

                    data.addField(targetField, geoData);
                } catch (IOException|GeoIp2Exception e) {
                    throw new RuntimeException("Unable to initialize geolite database", e);
                }
                return null;
            }
        });
    }

    public static class Builder implements Processor.Builder {

        private String ipField;
        private String databasePath;
        private String targetField;

        public void setIpField(String ipField) {
            this.ipField = ipField;
        }

        public void setDatabasePath(String dbPath) {
            this.databasePath = dbPath;
        }

        public void setTargetField(String targetField) {
            this.targetField = targetField;
        }

        public void fromMap(Map<String, Object> config) {
            this.ipField = (String) config.get("ip_field");
            this.targetField = (String) config.get("target_field");
            this.databasePath = (String) config.get("database_path");
        }

        @Override
        public Processor build() {
            return new GeoProcessor(ipField, databasePath, targetField);
        }

        public static class Factory implements Processor.Builder.Factory {

            @Override
            public Processor.Builder create() {
                return new Builder();
            }
        }

    }

}
