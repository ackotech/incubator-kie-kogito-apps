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
package org.kie.kogito.jobs.service.repository.impl;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.kie.kogito.jobs.service.model.JobServiceManagementInfo;
import org.kie.kogito.jobs.service.repository.JobServiceManagementRepository;
import org.kie.kogito.jobs.service.utils.DateUtil;

import io.quarkus.arc.DefaultBean;
import io.smallrye.mutiny.Uni;

import jakarta.enterprise.context.ApplicationScoped;

@DefaultBean
@ApplicationScoped
public class DefaultJobServiceManagementRepository implements JobServiceManagementRepository {

    private AtomicReference<JobServiceManagementInfo> instance = new AtomicReference<>(new JobServiceManagementInfo(null, null, null));

    @Override
    public Uni<JobServiceManagementInfo> getAndUpdate(String id, Function<JobServiceManagementInfo, JobServiceManagementInfo> computeUpdate) {
        return set(computeUpdate.apply(instance.get()));
    }

    @Override
    public Uni<JobServiceManagementInfo> set(JobServiceManagementInfo info) {
        instance.set(info);
        return Uni.createFrom().item(instance.get());
    }

    @Override
    public Uni<JobServiceManagementInfo> heartbeat(JobServiceManagementInfo info) {
        info.setLastHeartbeat(DateUtil.now().toOffsetDateTime());
        return set(info);
    }

    @Override
    public Uni<Void> ensureRowExists(String id) {
        JobServiceManagementInfo current = instance.get();
        if (current == null || current.getId() == null) {
            instance.set(new JobServiceManagementInfo(id, null, null));
        }
        return Uni.createFrom().voidItem();
    }

    @Override
    public Uni<JobServiceManagementInfo> claim(String id, String token, long heartbeatExpirationInSeconds) {
        JobServiceManagementInfo current = instance.get();
        if (current == null || current.getId() == null) {
            instance.set(new JobServiceManagementInfo(id, token, DateUtil.now().toOffsetDateTime()));
            return Uni.createFrom().item(instance.get());
        }
        boolean canClaim = current.getToken() == null || token.equals(current.getToken()) ||
                current.getLastHeartbeat() == null ||
                current.getLastHeartbeat().isBefore(DateUtil.now().toOffsetDateTime().minusSeconds(heartbeatExpirationInSeconds));
        if (canClaim) {
            JobServiceManagementInfo updated = new JobServiceManagementInfo(id, token, DateUtil.now().toOffsetDateTime());
            instance.set(updated);
            return Uni.createFrom().item(updated);
        }
        return Uni.createFrom().nullItem();
    }

    @Override
    public Uni<JobServiceManagementInfo> release(String id, String token) {
        JobServiceManagementInfo current = instance.get();
        if (current != null && id.equals(current.getId()) && token.equals(current.getToken())) {
            JobServiceManagementInfo updated = new JobServiceManagementInfo(id, null, null);
            instance.set(updated);
            return Uni.createFrom().item(updated);
        }
        return Uni.createFrom().nullItem();
    }

    @Override
    public Uni<JobServiceManagementInfo> forceClaim(String id, String token) {
        JobServiceManagementInfo updated = new JobServiceManagementInfo(id, token, DateUtil.now().toOffsetDateTime());
        instance.set(updated);
        return Uni.createFrom().item(updated);
    }
}
