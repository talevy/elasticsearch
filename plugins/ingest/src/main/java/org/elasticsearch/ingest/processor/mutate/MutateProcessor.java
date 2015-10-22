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

package org.elasticsearch.ingest.processor.mutate;

import org.elasticsearch.ingest.Data;
import org.elasticsearch.ingest.processor.Processor;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public final class MutateProcessor implements Processor {

    public static final String TYPE = "mutate";

    private final Map<String, Object> update;
    private final Map<String, String> rename;
    private final Map<String, String> convert;
    private final Map<String, String> split;
    private final Map<String, String> gsub;
    private final Map<String, String> join;
    private final Map<String, String> merge;
    private final List<String> remove;
    private final List<String> strip;
    private final List<String> uppercase;
    private final List<String> lowercase;

    public MutateProcessor(Map<String, Object> update,
                           Map<String, String> rename,
                           Map<String, String> convert,
                           Map<String, String> split,
                           Map<String, String> gsub,
                           Map<String, String> join,
                           Map<String, String> merge,
                           List<String> remove,
                           List<String> strip,
                           List<String> uppercase,
                           List<String> lowercase) {
        this.update = update;
        this.rename = rename;
        this.convert = convert;
        this.split = split;
        this.gsub = gsub;
        this.join = join;
        this.merge = merge;
        this.remove = remove;
        this.strip = strip;
        this.uppercase = uppercase;
        this.lowercase = lowercase;
    }

    @Override
    public void execute(Data data) {
        doUpdate(data);
        doRename(data);
        doConvert(data);
        doSplit(data);
        doGsub(data);
        doJoin(data);
        doMerge(data);
        doRemove(data);
        doStrip(data);
        doUppercase(data);
        doLowercase(data);
    }

    private void doUpdate(Data data) {
        if (update == null) { return; }

        for(Map.Entry<String, Object> entry : update.entrySet()) {
            data.addField(entry.getKey(), entry.getValue());
        }
    }

    private void doRename(Data data) {
        if (rename == null) { return; }

        for(Map.Entry<String, String> entry : rename.entrySet()) {
            Object oldVal = data.getProperty(entry.getKey());
            data.addField(entry.getValue(), oldVal);
        }
    }

    private void doConvert(Data data) {
        if (convert == null) { return; }

        for(Map.Entry<String, String> entry : convert.entrySet()) {
            String toType = entry.getValue();

            Object oldVal = data.getProperty(entry.getKey());

            if("integer".equals(toType)) {
                oldVal = Integer.parseInt(oldVal.toString());
            } else if("float".equals(toType)) {
                oldVal = Float.parseFloat(oldVal.toString());
            } else if("string".equals(toType)) {
                oldVal = oldVal.toString();
            } else if("boolean".equals(toType)) {
                String val = oldVal.toString();
                if (val.equals("true") || val.equals("t") || val.equals("yes") || val.equals("y") || val.equals("1")) {
                    oldVal = true;
                } else if (val.equals("false") || val.equals("f") || val.equals("no") || val.equals("n") || val.equals("0")) {
                    oldVal = false;
                } else {
                    // TODO(talevy): throw exception
                }
            } else {
                // TODO(talevy): throw exception
            }

            data.addField(entry.getKey(), oldVal);
        }
    }

    private void doSplit(Data data) {
        if (split == null) { return; }

        for(Map.Entry<String, String> entry : split.entrySet()) {
            Object oldVal = data.getProperty(entry.getKey());
            if (oldVal instanceof String) {
                data.addField(entry.getKey(), Arrays.asList(((String) oldVal).split(entry.getValue())));
            } else {
                // TODO(talevy): throw exception
            }
        }
    }

    private void doGsub(Data data) {
        if (gsub == null) { return; }
        throw new UnsupportedOperationException();
    }

    private void doJoin(Data data) {
        if (join == null) { return; }

        for(Map.Entry<String, String> entry : join.entrySet()) {
            Object oldVal = data.getProperty(entry.getKey());
            if (oldVal instanceof List) {
                String joined = (String) ((List) oldVal)
                        .stream()
                        .map(Object::toString)
                        .collect(Collectors.joining(entry.getValue()));

                data.addField(entry.getKey(), joined);
            } else {
                // TODO(talevy): throw exception
            }
        }
    }

    private void doMerge(Data data) {
        if (merge == null) { return; }
        throw new UnsupportedOperationException();
    }

    private void doRemove(Data data) {
        if (remove == null) { return; }
        for(String field : remove) {
            data.getDocument().remove(field);
        }
    }

    private void doStrip(Data data) {
        if (strip == null) { return; }

        for(String field : strip) {
            Object val = data.getProperty(field);
            if (val instanceof String) {
                data.addField(field, ((String) val).trim());
            } else {
                // TODO(talevy): throw exception
            }
        }
    }

    private void doUppercase(Data data) {
        if (uppercase == null) { return; }

        for(String field : uppercase) {
            Object val = data.getProperty(field);
            if (val instanceof String) {
                data.addField(field, ((String) val).toUpperCase());
            } else {
                // TODO(talevy): throw exception
            }
        }
    }

    private void doLowercase(Data data) {
        if (lowercase == null) { return; }

        for(String field : lowercase) {
            Object val = data.getProperty(field);
            if (val instanceof String) {
                data.addField(field, ((String) val).toLowerCase());
            } else {
                // TODO(talevy): throw exception
            }
        }
    }

    public static class Builder implements Processor.Builder {

        private Map<String, Object> update;
        private Map<String, String> rename;
        private Map<String, String> convert;
        private Map<String, String> split;
        private Map<String, String> gsub;
        private Map<String, String> trim;
        private Map<String, String> join;
        private Map<String, String> merge;
        private List<String> remove;
        private List<String> strip;
        private List<String> uppercase;
        private List<String> lowercase;

        public void setUpdate(Map<String, Object> update) {
            this.update = update;
        }

        public void setRename(Map<String, String> rename) {
            this.rename = rename;
        }

        public void setConvert(Map<String, String> convert) {
            this.convert = convert;
        }

        public void setSplit(Map<String, String> split) {
            this.split = split;
        }

        public void setGsub(Map<String, String> gsub) {
            this.gsub = gsub;
        }

        public void setJoin(Map<String, String> join) {
            this.join = join;
        }

        public void setMerge(Map<String, String> merge) {
            this.merge = merge;
        }

        public void setRemove(List<String> remove) {
            this.remove = remove;
        }

        public void setStrip(List<String> strip) {
            this.strip = strip;
        }

        public void setUppercase(List<String> uppercase) {
            this.uppercase = uppercase;
        }

        public void setLowercase(List<String> lowercase) {
            this.lowercase = lowercase;
        }

        public void fromMap(Map<String, Object> config) {
            this.update = (Map<String, Object>) config.get("update");
            this.rename = (Map<String, String>) config.get("rename");
            this.convert = (Map<String, String>) config.get("convert");
            this.split = (Map<String, String>) config.get("split");
            this.gsub = (Map<String, String>) config.get("gsub");
            this.join = (Map<String, String>) config.get("join");
            this.merge = (Map<String, String>) config.get("merge");
            this.remove = (List<String>) config.get("remove");
            this.strip = (List<String>) config.get("strip");
            this.uppercase = (List<String>) config.get("uppercase");
            this.lowercase = (List<String>) config.get("lowercase");
        }

        @Override
        public Processor build() {
            return new MutateProcessor(update, rename, convert, split, gsub,
                    join,merge, remove, strip, uppercase, lowercase);
        }

        public static class Factory implements Processor.Builder.Factory {

            @Override
            public Processor.Builder create() {
                return new Builder();
            }
        }

    }

}
