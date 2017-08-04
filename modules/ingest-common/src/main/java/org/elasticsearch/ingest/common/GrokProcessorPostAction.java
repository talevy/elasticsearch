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

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.ingest.common.IngestCommonPlugin.GROK_PATTERNS;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestStatus.OK;

public class GrokProcessorPostAction extends Action<GrokProcessorPostAction.Request,
    GrokProcessorPostAction.Response, GrokProcessorPostAction.RequestBuilder> {

    public static final GrokProcessorPostAction INSTANCE = new GrokProcessorPostAction();
    public static final String NAME = "cluster:admin/ingest/processor/grok/post";
    public static final GrokDiscover GROK_DISCOVER = new GrokDiscover(GROK_PATTERNS);

    private GrokProcessorPostAction() {
        super(NAME);
    }

    @Override
    public RequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new RequestBuilder(client);
    }

    @Override
    public Response newResponse() {
        return new Response(null);
    }

    public static class Request extends ActionRequest {
        private String input;

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public Request() {

        }

        public Request(String input) {
            this.input = input;
        }

        public String getInput() {
            return input;
        }
    }

    public static class RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder> {
        public RequestBuilder(ElasticsearchClient client) {
            super(client, GrokProcessorPostAction.INSTANCE, new Request());
        }
    }

    public static class Response extends AcknowledgedResponse implements ToXContentObject {
        private List<GrokDiscover.GrokDiscoveryCandidate> discoverCandidates;

        public Response(List<GrokDiscover.GrokDiscoveryCandidate> discoverCandidates) {
            this.discoverCandidates = discoverCandidates;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(); {
                builder.array("hits", discoverCandidates);
            } builder.endObject();
            return builder;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            discoverCandidates = in.readStreamableList(GrokDiscover.GrokDiscoveryCandidate::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStreamableList(discoverCandidates);
        }
    }

    public static class TransportAction extends HandledTransportAction<Request, Response> {

        @Inject
        public TransportAction(Settings settings, ThreadPool threadPool, TransportService transportService,
                               ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
            super(settings, NAME, threadPool, transportService, actionFilters,
                indexNameExpressionResolver, Request::new);
        }

        @Override
        protected void doExecute(Request request, ActionListener<Response> listener) {
            try {
                listener.onResponse(new Response(GROK_DISCOVER.discover(request.getInput())));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }
    }

    public static class RestAction extends BaseRestHandler {
        public RestAction(Settings settings, RestController controller) {
            super(settings);
            controller.registerHandler(POST, "/_ingest/processor/grok", this);
        }

        @Override
        public String getName() {
            return "ingest_processor_grok_post";
        }

        @Override
        protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
            Request request = new Request(restRequest.param("input"));
            return channel -> client.executeLocally(INSTANCE, request,new RestBuilderListener<Response>(channel) {
                @Override
                public RestResponse buildResponse(Response response, XContentBuilder builder) throws Exception {
                    response.toXContent(builder, ToXContent.EMPTY_PARAMS);
                    return new BytesRestResponse(OK, builder);
                }
            });
        }
    }
}
