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
package org.kie.kogito.jobs.service.repository.postgresql;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kie.kogito.jobs.service.model.JobServiceManagementInfo;
import org.kie.kogito.jobs.service.repository.JobServiceManagementRepository;
import org.kie.kogito.jobs.service.utils.DateUtil;
import org.kie.kogito.testcontainers.quarkus.PostgreSqlQuarkusTestResource;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;

import jakarta.inject.Inject;

import static org.assertj.core.api.Assertions.assertThat;

@QuarkusTest
@QuarkusTestResource(PostgreSqlQuarkusTestResource.class)
class PostgreSqlJobServiceManagementRepositoryTest {

    @Inject
    JobServiceManagementRepository tested;

    @BeforeEach
    void setUp() {
    }

    @Test
    void testGetAndUpdate() {
        String id = "instance-id-1";
        String token = "token1";
        tested.ensureRowExists(id).await().indefinitely();
        JobServiceManagementInfo claimed = tested.claim(id, token, 1).await().indefinitely();
        assertThat(claimed).isNotNull();
        assertThat(claimed.getId()).isEqualTo(id);
        assertThat(claimed.getToken()).isEqualTo(token);
    }

    @Test
    void testClaimFailsWhenActive() {
        String id = "instance-id-2";
        tested.ensureRowExists(id).await().indefinitely();
        JobServiceManagementInfo first = tested.claim(id, "tokenA", 1000).await().indefinitely();
        assertThat(first).isNotNull();
        JobServiceManagementInfo second = tested.claim(id, "tokenB", 1000).await().indefinitely();
        assertThat(second).isNull();
    }

    private JobServiceManagementInfo create(String id, String token) {
        tested.ensureRowExists(id).await().indefinitely();
        JobServiceManagementInfo created = tested.claim(id, token, 1).await().indefinitely();
        assertThat(created.getId()).isEqualTo(id);
        assertThat(created.getToken()).isEqualTo(token);
        return created;
    }

    @Test
    void testHeartbeat() {
        String id = "instance-id-3";
        String token = "token3";
        JobServiceManagementInfo created = create(id, token);

        JobServiceManagementInfo updated = tested.heartbeat(created).await().indefinitely();
        assertThat(updated.getLastHeartbeat()).isNotNull();
        assertThat(updated.getLastHeartbeat()).isBefore(DateUtil.now().plusSeconds(1).toOffsetDateTime());
    }

    @Test
    void testConflictHeartbeat() {
        String id = "instance-id-4";
        String token = "token4";
        create(id, token);

        JobServiceManagementInfo updated = tested.heartbeat(new JobServiceManagementInfo(id, "differentToken", null)).await().indefinitely();
        assertThat(updated).isNull();
    }
}
