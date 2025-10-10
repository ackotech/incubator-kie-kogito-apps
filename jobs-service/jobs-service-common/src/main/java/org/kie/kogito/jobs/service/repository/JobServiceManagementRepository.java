/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.kie.kogito.jobs.service.repository;

import java.util.function.Function;

import org.kie.kogito.jobs.service.model.JobServiceManagementInfo;

import io.smallrye.mutiny.Uni;

public interface JobServiceManagementRepository {

    Uni<JobServiceManagementInfo> getAndUpdate(String id, Function<JobServiceManagementInfo, JobServiceManagementInfo> computeUpdate);

    Uni<JobServiceManagementInfo> set(JobServiceManagementInfo info);

    Uni<JobServiceManagementInfo> heartbeat(JobServiceManagementInfo info);

    /**
     * Ensure the management row exists with NULL token/heartbeat to avoid first-claim races.
     */
    Uni<Void> ensureRowExists(String id);

    /**
     * Attempt to atomically claim leadership.
     * Returns updated info when claim succeeds, or null when not leader.
     */
    Uni<JobServiceManagementInfo> claim(String id, String token, long heartbeatExpirationInSeconds);

    /**
     * Release leadership by clearing token and last_heartbeat if token matches.
     * Returns updated (now cleared) info when release succeeds, or null if token mismatch.
     */
    Uni<JobServiceManagementInfo> release(String id, String token);

    /**
     * Forcefully claim leadership by setting the token regardless of current holder.
     * Returns the updated info.
     */
    Uni<JobServiceManagementInfo> forceClaim(String id, String token);

}
