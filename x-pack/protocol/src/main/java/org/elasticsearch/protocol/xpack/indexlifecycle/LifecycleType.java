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
package org.elasticsearch.protocol.xpack.indexlifecycle;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface LifecycleType {
    /**
     * @return the first phase of this policy to execute
     */
    List<Phase> getOrderedPhases(Map<String, Phase> phases);

    /**
     * Returns the next phase thats available after
     * <code>currentPhaseName</code>. Note that <code>currentPhaseName</code>
     * does not need to exist in <code>phases</code>.
     *
     * If the current {@link Phase} is the last phase in the {@link LifecyclePolicy} this
     * method will return <code>null</code>.
     *
     * If the phase is not valid for the lifecycle type an
     * {@link IllegalArgumentException} will be thrown.
     */
    String getNextPhaseName(String currentPhaseName, Map<String, Phase> phases);

    /**
     * Returns the previous phase thats available before
     * <code>currentPhaseName</code>. Note that <code>currentPhaseName</code>
     * does not need to exist in <code>phases</code>.
     *
     * If the current {@link Phase} is the first phase in the {@link LifecyclePolicy}
     * this method will return <code>null</code>.
     *
     * If the phase is not valid for the lifecycle type an
     * {@link IllegalArgumentException} will be thrown.
     */
    String getPreviousPhaseName(String currentPhaseName, Map<String, Phase> phases);

    List<LifecycleAction> getOrderedActions(Phase phase);

    /**
     * Returns the name of the next phase that is available in the phases after
     * <code>currentActionName</code>. Note that <code>currentActionName</code>
     * does not need to exist in the {@link Phase}.
     *
     * If the current action is the last action in the phase this method will
     * return <code>null</code>.
     *
     * If the action is not valid for the phase an
     * {@link IllegalArgumentException} will be thrown.
     */
    String getNextActionName(String currentActionName, Phase phase);


    /**
     * validates whether the specified <code>phases</code> are valid for this
     * policy instance.
     *
     * @param phases
     *            the phases to verify validity against
     * @throws IllegalArgumentException
     *             if a specific phase or lack of a specific phase is invalid.
     */
    void validate(Collection<Phase> phases);
}
