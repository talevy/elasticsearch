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

package org.elasticsearch.ingest.common;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;

import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.ingest.ConfigurationUtils.newConfigurationException;

/**
 * This processor creates hashes (fingerprints) of one or more fields and stores
 * the result in a new field
 */
public final class FingerprintProcessor extends AbstractProcessor {
    enum Method {
        SHA1 {
            @Override
            public Object hash(String value, String key, int iterations) throws Exception {
                return hexOf(value, key, iterations,"PBKDF2withHMACSHA1");
            }
        }, SHA256 {
            @Override
            public Object hash(String value, String key, int iterations) throws Exception {
                return hexOf(value, key, iterations,"PBKDF2withHMACSHA256");
            }
        }, SHA384 {
            @Override
            public Object hash(String value, String key, int iterations) throws Exception {
                return hexOf(value, key, iterations,"PBKDF2withHMACSHA384");
            }
        }, SHA512 {
            @Override
            public Object hash(String value, String key, int iterations) throws Exception {
                return hexOf(value, key, iterations,"PBKDF2withHMACSHA512");
            }
        }, MD5 {
            @Override
            public Object hash(String value, String key, int iterations) throws Exception {
                return hexOf(value, key, iterations,"PBKDF2withHMACMD5");
            }
        }, MURMUR3 {
            @Override
            public Object hash(String value, String key, int iterations) {
                throw new UnsupportedOperationException("murmur3 not implemented");
            }
        }, UUID {
            @Override
            public Object hash(String value, String key, int iterations) {
                return UUIDs.randomBase64UUID();
            }
        };

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }

        public abstract Object hash(String value, String key, int iterations) throws Exception;

        public static Method fromString(String processorTag, String propertyName, String type) {
            try {
                return Method.valueOf(type.toUpperCase(Locale.ROOT));
            } catch(IllegalArgumentException e) {
                throw newConfigurationException(TYPE, processorTag, propertyName, "type [" + type +
                        "] not supported, cannot convert field.");
            }
        }

        protected String hexOf(String value, String key, int iterations, String algorithm) throws Exception {
            SecretKeyFactory secretKeyFactory = SecretKeyFactory.getInstance(algorithm);
            PBEKeySpec keySpec = new PBEKeySpec(value.toCharArray(), key.getBytes(), iterations, 128);
            SecretKey tmp = secretKeyFactory.generateSecret(keySpec);
            return MessageDigests.toHexString(tmp.getEncoded());

        }
    }

    public static final String TYPE = "fingerprint";

    private final List<String> fields;
    private final String targetField;
    private final Method method;
    private final String key;
    private final String salt;
    private final int iterations;
    private final boolean concatenateFields;

    FingerprintProcessor(String tag, List<String> fields, String targetField, Method method, String key,
                         String salt, boolean concatenateFields, int iterations) {
        super(tag);
        this.fields = fields;
        this.targetField = targetField;
        this.method = method;
        this.key = key;
        this.salt = salt;
        this.concatenateFields = concatenateFields;
        this.iterations = iterations;
    }

    List<String> getFields() {
        return fields;
    }

    String getTargetField() {
        return targetField;
    }

    @Override
    public void execute(IngestDocument document) {
        if (concatenateFields) {
            String concatenatedFieldValue = fields.stream().map(f -> document.getFieldValue(f, String.class)).collect(Collectors.joining());
            try {
                document.setFieldValue(targetField, method.hash(concatenatedFieldValue, key, iterations));
            } catch (Exception e) {
                throw new IllegalArgumentException("fields could not be hashed", e);
            }
        } else {
            List<Object> hashedFieldValues = fields.stream().map(f -> {
                try {
                    return method.hash(document.getFieldValue(f, String.class), key, iterations);
                } catch (Exception e) {
                    throw new IllegalArgumentException("field[" + f + "] could not be hashed", e);
                }
            }).collect(Collectors.toList());
            document.setFieldValue(targetField, hashedFieldValues);
        }
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory implements Processor.Factory {
        @Override
        public FingerprintProcessor create(Map<String, Processor.Factory> registry, String processorTag, Map<String, Object> config) {
            List<String> field = ConfigurationUtils.readList(TYPE, processorTag, config, "fields");
            String targetField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "target_field");
            String key = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "key");
            String salt = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "salt");
            boolean concatenateFields = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config,"concatenate_fields", false);
            String methodProperty = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "method");
            Method method = Method.fromString(processorTag, "method", methodProperty);
            int iterations = ConfigurationUtils.readIntProperty(TYPE, processorTag, config, "iterations", 1);
            return new FingerprintProcessor(processorTag, field, targetField, method, key, salt, concatenateFields, iterations);
        }
    }
}
