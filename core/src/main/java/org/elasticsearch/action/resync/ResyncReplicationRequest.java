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
package org.elasticsearch.action.resync;

import org.elasticsearch.action.support.replication.ReplicatedWriteRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;

import java.io.IOException;
import java.util.List;

public final class ResyncReplicationRequest extends ReplicatedWriteRequest<ResyncReplicationRequest> {

    private List<Translog.Operation> operations;

    ResyncReplicationRequest() {
        super();
    }

    ResyncReplicationRequest(StreamInput in) throws IOException {
        super(in);
        operations = in.readList(Translog.Operation::readType);
    }

    public ResyncReplicationRequest(ShardId shardId, List<Translog.Operation> operations) {
        super(shardId);
        this.operations = operations;
    }

    public List<Translog.Operation> getOperations() {
        return operations;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeList(operations);
    }

    @Override
    public String toString() {
        return "TransportResyncReplicationAction.Request{" +
            "shardId=" + shardId +
            ", timeout=" + timeout +
            ", index='" + index + '\'' +
            ", ops=" + operations.size() +
            "}";
    }
}
