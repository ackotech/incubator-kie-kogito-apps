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
package org.kie.kogito.jobs.service.management;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.kie.kogito.jobs.service.messaging.MessagingHandler;
import org.kie.kogito.jobs.service.model.JobServiceManagementInfo;
import org.kie.kogito.jobs.service.repository.JobServiceManagementRepository;
import org.kie.kogito.jobs.service.utils.DateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.TimeoutStream;
import io.vertx.mutiny.core.Vertx;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Event;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;

@ApplicationScoped
public class JobServiceInstanceManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(JobServiceInstanceManager.class);

    @ConfigProperty(name = "kogito.jobs-service.management.heartbeat.interval-in-seconds", defaultValue = "1")
    long heardBeatIntervalInSeconds;

    @ConfigProperty(name = "kogito.jobs-service.management.leader-check.interval-in-seconds", defaultValue = "1")
    long leaderCheckIntervalInSeconds;

    @ConfigProperty(name = "kogito.jobs-service.management.heartbeat.expiration-in-seconds", defaultValue = "10")
    long heartbeatExpirationInSeconds;

    @ConfigProperty(name = "kogito.jobs-service.management.heartbeat.management-id", defaultValue = "kogito-jobs-service-leader")
    String leaderManagementId;

    @Inject
    Instance<MessagingHandler> messagingHandlerInstance;

    @Inject
    Event<MessagingChangeEvent> messagingChangeEventEvent;
    @Inject
    Event<JobServiceInstanceInfoEvent> instanceInfoEvent;

    @Inject
    Vertx vertx;

    @Inject
    JobServiceManagementRepository repository;

    private TimeoutStream checkLeader;

    private TimeoutStream heartbeat;

    private final AtomicReference<JobServiceManagementInfo> currentInfo = new AtomicReference<>();

    private final AtomicBoolean leader = new AtomicBoolean(false);

    void startup(@Observes StartupEvent startupEvent) {
        buildAndSetInstanceInfo();
        // ensure the management row exists to avoid first-claim races
        repository.ensureRowExists(leaderManagementId).subscribe().with(v -> LOGGER.debug("Ensured management row exists for id {}", leaderManagementId),
                ex -> LOGGER.error("Failed to ensure management row exists", ex));

        //background task for leader check, it will be started after the first tryBecomeLeader() execution
        checkLeader = vertx.periodicStream(TimeUnit.SECONDS.toMillis(leaderCheckIntervalInSeconds))
                .handler(id -> tryBecomeLeader(currentInfo.get(), checkLeader, heartbeat)
                        .subscribe().with(i -> LOGGER.trace("Leader check completed"),
                                ex -> LOGGER.error("Error checking Leader", ex)))
                .pause();

        //background task for heartbeat will be started when become leader
        heartbeat = vertx.periodicStream(TimeUnit.SECONDS.toMillis(heardBeatIntervalInSeconds))
                .handler(t -> heartbeat(currentInfo.get())
                        .subscribe().with(i -> LOGGER.trace("Heartbeat completed {}", currentInfo.get()),
                                ex -> LOGGER.error("Error on heartbeat {}", currentInfo.get(), ex)))
                .pause();

        //initial leader check
        tryBecomeLeader(currentInfo.get(), checkLeader, heartbeat)
                .subscribe().with(i -> LOGGER.info("Initial leader check completed"),
                        ex -> LOGGER.error("Error on initial check leader", ex));
    }

    private void disableCommunication() {
        //disable consuming events
        messagingHandlerInstance.stream().forEach(MessagingHandler::pause);
        //disable producing events
        messagingChangeEventEvent.fire(new MessagingChangeEvent(false));

        LOGGER.warn("Disabled communication not leader instance");
    }

    private void enableCommunication() {
        //enable consuming events
        messagingHandlerInstance.stream().forEach(MessagingHandler::resume);
        //enable producing events
        messagingChangeEventEvent.fire(new MessagingChangeEvent(true));

        LOGGER.info("Enabled communication for leader instance");
    }

    void onShutdown(@Observes ShutdownEvent event) {
        shutdown();
    }

    void onReleaseLeader(@Observes ReleaseLeaderEvent event) {
        shutdown();
    }

    void onResignLeader(@Observes ResignLeaderEvent event) {
        // Release leadership but keep timers alive; we want to re-enter follower state and compete again
        release(currentInfo.get())
                .subscribe().with(i -> LOGGER.info("Resign completed; switching to follower and resuming leader checks"),
                        ex -> LOGGER.error("Error on resign leader", ex));
    }

    private void shutdown() {
        release(currentInfo.get())
                .onItem().invoke(i -> checkLeader.cancel())
                .onItem().invoke(i -> heartbeat.cancel())
                .subscribe().with(i -> LOGGER.info("Shutting down leader instance check"),
                        ex -> LOGGER.error("Shutdown error", ex));
    }

    protected boolean isLeader() {
        return leader.get();
    }

    protected Uni<JobServiceManagementInfo> tryBecomeLeader(JobServiceManagementInfo info, TimeoutStream checkLeader, TimeoutStream heartbeat) {
        LOGGER.debug("Try to become Leader");
        return repository.claim(info.getId(), info.getToken(), heartbeatExpirationInSeconds)
                .onItem().transformToUni(claimed -> {
                    if (claimed != null) {
                        info.setLastHeartbeat(DateUtil.now().toOffsetDateTime());
                        LOGGER.info("SET Leader {}", info);
                        leader.set(true);
                        enableCommunication();
                        heartbeat.resume();
                        checkLeader.pause();
                        instanceInfoEvent.fire(new JobServiceInstanceInfoEvent(info.getId(), info.getToken()));
                        return Uni.createFrom().item(info);
                    } else {
                        if (isLeader()) {
                            LOGGER.info("Not Leader");
                            leader.set(false);
                            disableCommunication();
                        }
                        heartbeat.pause();
                        checkLeader.resume();
                        // add jittered backoff by pausing and resuming after random delay
                        long backoffMillis = computeJitteredBackoffMillis();
                        checkLeader.pause();
                        vertx.setTimer(backoffMillis, t -> checkLeader.resume());
                        return Uni.createFrom().nullItem();
                    }
                });
    }

    protected Uni<Void> release(JobServiceManagementInfo info) {
        leader.set(false);
        return repository.release(info.getId(), info.getToken())
                .onItem().invoke(i -> disableCommunication())
                .onItem().invoke(i -> {
                    LOGGER.info("Leader instance released");
                    long backoffMillis = computeJitteredBackoffMillis();
                    heartbeat.pause();
                    checkLeader.pause();
                    vertx.setTimer(backoffMillis, t -> checkLeader.resume());
                })
                .onFailure().invoke(ex -> LOGGER.error("Error releasing leader"))
                .replaceWithVoid();
    }

    protected Uni<JobServiceManagementInfo> heartbeat(JobServiceManagementInfo info) {
        if (!isLeader()) {
            return Uni.createFrom().nullItem();
        }
        return repository.heartbeat(info)
                .onItem().transformToUni(updated -> {
                    if (updated == null) {
                        // demote if our token was replaced
                        LOGGER.warn("Heartbeat failed; demoting from leader: {}", info);
                        leader.set(false);
                        disableCommunication();
                        heartbeat.pause();
                        long backoffMillis = computeJitteredBackoffMillis();
                        checkLeader.pause();
                        vertx.setTimer(backoffMillis, t -> checkLeader.resume());
                        return Uni.createFrom().nullItem();
                    }
                    return Uni.createFrom().item(updated);
                });
    }

    private void buildAndSetInstanceInfo() {
        currentInfo.set(new JobServiceManagementInfo(leaderManagementId, generateToken(), DateUtil.now().toOffsetDateTime()));
        LOGGER.info("Current Job Service Instance {}", currentInfo.get());
        instanceInfoEvent.fire(new JobServiceInstanceInfoEvent(currentInfo.get().getId(), currentInfo.get().getToken()));
    }

    private String generateToken() {
        return UUID.randomUUID().toString();
    }

    private long computeJitteredBackoffMillis() {
        long base = TimeUnit.SECONDS.toMillis(leaderCheckIntervalInSeconds);
        long multiplier = 1 + (long) (Math.random() * 3); // 1..3
        long jitter = (long) (base * 0.1 * (Math.random() - 0.5)); // +/-10%
        return Math.max(1, multiplier * base + jitter);
    }

    protected JobServiceManagementInfo getCurrentInfo() {
        return currentInfo.get();
    }

    protected TimeoutStream getCheckLeader() {
        return checkLeader;
    }

    protected TimeoutStream getHeartbeat() {
        return heartbeat;
    }
}
