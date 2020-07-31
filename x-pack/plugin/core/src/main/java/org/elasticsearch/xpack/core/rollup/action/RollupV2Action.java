/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.rollup.action;


import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.tasks.BaseTasksResponse;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.rollup.RollupField;
import org.elasticsearch.xpack.core.rollup.job.RollupV2Job;

import java.io.IOException;
import java.util.Collections;
import java.util.Objects;

public class RollupV2Action extends ActionType<RollupV2Action.Response> {

    public static final RollupV2Action INSTANCE = new RollupV2Action();
    public static final String NAME = "cluster:admin/xpack/rollupV2";

    private RollupV2Action() {
        super(NAME, RollupV2Action.Response::new);
    }

    public static class Request extends AcknowledgedRequest<Request> implements ToXContentObject {
        private String index;
        private RollupV2Job rollupJob;

        public Request(String index, RollupV2Job rollupJob) {
            this.index = index;
            this.rollupJob = rollupJob;
        }

        public Request() {}

        public Request(StreamInput in) throws IOException {
            super(in);
            index = in.readString();
            rollupJob = new RollupV2Job(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(index);
            rollupJob.writeTo(out);
        }

        public String getIndex() {
            return index;
        }

        public RollupV2Job getRollupJob() {
            return rollupJob;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(RollupField.ID.getPreferredName(), index);
            rollupJob.toXContent(builder, params);
            builder.endObject();
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(index, rollupJob);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Request other = (Request) obj;
            return Objects.equals(index, other.index) && Objects.equals(rollupJob, other.rollupJob);
        }
    }

    public static class RequestBuilder extends ActionRequestBuilder<Request, Response> {

        protected RequestBuilder(ElasticsearchClient client, RollupV2Action action) {
            super(client, action, new Request());
        }
    }

    public static class Response extends BaseTasksResponse implements Writeable, ToXContentObject {

        private final boolean started;

        public Response(boolean started) {
            super(Collections.emptyList(), Collections.emptyList());
            this.started = started;
        }

        public Response(StreamInput in) throws IOException {
            super(in);
            started = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBoolean(started);
        }

        public boolean isStarted() {
            return started;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("started", started);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Response response = (Response) o;
            return started == response.started;
        }

        @Override
        public int hashCode() {
            return Objects.hash(started);
        }
    }
}
