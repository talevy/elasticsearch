/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.rollup.v2;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.Assignment;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.rollup.job.RollupV2Job;
import org.elasticsearch.xpack.core.rollup.job.RollupV2JobStatus;

import java.util.Collections;
import java.util.Map;

public class RollupV2TaskExecutor extends PersistentTasksExecutor<RollupV2Job> {

    private final Client client;
    private final ThreadPool threadPool;

    public RollupV2TaskExecutor(String taskName, String executor, Client client, ThreadPool threadPool) {
        super(taskName, executor);
        this.client = client;
        this.threadPool = threadPool;
    }


    @Override
    public Assignment getAssignment(final RollupV2Job job, final ClusterState clusterState) {
        final DiscoveryNode node = selectLeastLoadedNode(clusterState, DiscoveryNode::isDataNode);
        if (node == null) {
            return null;
        } else {
            return new Assignment(node.getId(), "node is the least loaded data node and remote cluster client");
        }
    }

    @Override
    protected void nodeOperation(AllocatedPersistentTask task, RollupV2Job params, PersistentTaskState state) {
        RollupV2JobTask job = (RollupV2JobTask) task;
        job.start(ActionListener.wrap(r -> {
            System.out.println("here");
        }, e -> {

        }));
    }

    @Override
    protected AllocatedPersistentTask createTask(long id, String type, String action, TaskId parentTaskId,
                                                 PersistentTasksCustomMetadata.PersistentTask<RollupV2Job> persistentTask,
                                                 Map<String, String> headers) {
        return new RollupV2JobTask(id, type, action, parentTaskId, persistentTask.getParams(),
            (RollupV2JobStatus) persistentTask.getState(), client, threadPool, Collections.emptyMap());
    }
}
