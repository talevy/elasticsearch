package org.elasticsearch.xpack.core.indexlifecycle.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.indexlifecycle.LifecyclePolicy;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class GetIndexLifecyclePolicyResponse extends ActionResponse implements ToXContentObject {

    private List<LifecyclePolicy> policies;

    public GetIndexLifecyclePolicyResponse() {
    }

    public GetIndexLifecyclePolicyResponse(List<LifecyclePolicy> policies) {
        this.policies = policies;
    }

    public List<LifecyclePolicy> getPolicies() {
        return policies;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        for (LifecyclePolicy policy : policies) {
            builder.field(policy.getName(), policy);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        policies = in.readList(LifecyclePolicy::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(policies);
    }

    @Override
    public int hashCode() {
        return Objects.hash(policies);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        GetIndexLifecyclePolicyResponse other = (GetIndexLifecyclePolicyResponse) obj;
        return Objects.equals(policies, other.policies);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

}
