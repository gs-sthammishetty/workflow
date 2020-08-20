/**
 * Copyright 2014 Nirmata, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nirmata.workflow.queue.zookeeper;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.nirmata.workflow.admin.WorkflowManagerState;
import com.nirmata.workflow.details.ZooKeeperConstants;
import com.nirmata.workflow.models.ExecutableTask;
import com.nirmata.workflow.models.TaskMode;
import com.nirmata.workflow.models.TaskType;
import com.nirmata.workflow.queue.QueueConsumer;
import com.nirmata.workflow.queue.TaskRunner;
import com.nirmata.workflow.serialization.Serializer;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.EnsurePath;
import org.apache.curator.utils.ThreadUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

// copied and modified from com.nirmata.workflow.queue.zookeeper.SimpleQueue
public class MultiQueueReader implements Closeable, QueueConsumer {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final CuratorFramework client;
    private final TaskRunner taskRunner;
    private final Serializer serializer;
    private final String primaryTaskPath;
    private final String primaryTaskLockPath;
    private final boolean primaryTaskIsIdempotent;
    private final TaskType primaryTaskType;


    private final String secondaryTaskPath;
    private final String secondaryTaskLockPath;
    private final boolean secondaryTaskIsIdempotent;
    private final TaskType secondaryTaskType;


    private final ExecutorService executorService = ThreadUtils.newSingleThreadExecutor("MultiQueueReader");
    private final AtomicBoolean started = new AtomicBoolean(false);

    private final EnsurePath primaryTaskEnsurePath;
    private final NodeFunc primaryTaskNodeFunc;
    private final KeyFunc primaryTaskKeyFunc;

    private final EnsurePath secondaryTaskEnsurePath;
    private final NodeFunc secondaryTaskNodeFunc;
    private final KeyFunc secondaryTaskKeyFunc;


    private final AtomicReference<WorkflowManagerState.State> state = new AtomicReference<>(WorkflowManagerState.State.LATENT);

    private static final String PREFIX = "qn-";
    private static final String SEPARATOR = "|";

    @FunctionalInterface
    private interface KeyFunc {
        String apply(String key, long value);
    }

    private static final Map<TaskMode, KeyFunc> keyFuncs;

    static {
        ImmutableMap.Builder<TaskMode, KeyFunc> builder = ImmutableMap.builder();
        builder.put(TaskMode.STANDARD, (key, value) -> key);
        builder.put(TaskMode.PRIORITY, (key, value) -> key + priorityToString(value));
        builder.put(TaskMode.DELAY, (key, value) -> key + epochToString(value));
        keyFuncs = builder.build();
    }

    private static class NodeAndDelay {
        final Optional<String> node;
        final Optional<Long> delay;

        public NodeAndDelay() {
            node = Optional.empty();
            delay = Optional.empty();
        }

        public NodeAndDelay(String node) {
            this.node = Optional.of(node);
            delay = Optional.empty();
        }

        public NodeAndDelay(Optional<String> node) {
            this.node = node;
            delay = Optional.empty();
        }

        public NodeAndDelay(long delay) {
            node = Optional.empty();
            this.delay = Optional.of(delay);
        }
    }

    @FunctionalInterface
    private interface NodeFunc {
        NodeAndDelay getNode(List<String> nodes);
    }

    private static final Map<TaskMode, NodeFunc> nodeFuncs;

    static {
        Random random = new Random();
        ImmutableMap.Builder<TaskMode, NodeFunc> builder = ImmutableMap.builder();
        builder.put(TaskMode.STANDARD, nodes -> new NodeAndDelay(nodes.get(random.nextInt(nodes.size()))));
        builder.put(TaskMode.PRIORITY, nodes -> new NodeAndDelay(nodes.stream().sorted().findFirst()));
        builder.put(TaskMode.DELAY, nodes -> {
            final long sortTime = System.currentTimeMillis();
            Optional<String> first = nodes.stream().sorted(delayComparator(sortTime)).findFirst();
            if (first.isPresent()) {
                long delay = getDelay(first.get(), sortTime);
                return (delay > 0) ? new NodeAndDelay(delay) : new NodeAndDelay(first.get());
            }
            return new NodeAndDelay();
        });
        nodeFuncs = builder.build();
    }

    private static Comparator<String> delayComparator(long sortTime) {
        return (s1, s2) -> {
            long diff = getDelay(s1, sortTime) - getDelay(s2, sortTime);
            return (diff < 0) ? -1 : ((diff > 0) ? 1 : 0);
        };
    }

    private static long getDelay(String itemNode, long sortTime) {
        long epoch = getEpoch(itemNode);
        return epoch - sortTime;
    }

    public MultiQueueReader(CuratorFramework client, TaskRunner taskRunner, Serializer serializer, MultiQueueInfo multiQueueInfo) {
        this.client = client;
        this.taskRunner = taskRunner;
        this.serializer = serializer;

        this.primaryTaskType = multiQueueInfo.getPrimaryTaskType();
        this.primaryTaskPath = ZooKeeperConstants.getQueuePath(primaryTaskType);
        this.primaryTaskLockPath = ZooKeeperConstants.getQueueLockPath(primaryTaskType);
        this.primaryTaskIsIdempotent = primaryTaskType.isIdempotent();
        primaryTaskEnsurePath = client.newNamespaceAwareEnsurePath(primaryTaskPath);
        primaryTaskNodeFunc = nodeFuncs.getOrDefault(multiQueueInfo.getPrimaryTaskType().getMode(), nodeFuncs.get(TaskMode.STANDARD));
        primaryTaskKeyFunc = keyFuncs.getOrDefault(multiQueueInfo.getPrimaryTaskType().getMode(), keyFuncs.get(TaskMode.STANDARD));

        this.secondaryTaskType = multiQueueInfo.getSecondaryTaskType();
        this.secondaryTaskPath = ZooKeeperConstants.getQueuePath(secondaryTaskType);
        this.secondaryTaskLockPath = ZooKeeperConstants.getQueueLockPath(secondaryTaskType);
        this.secondaryTaskIsIdempotent = secondaryTaskType.isIdempotent();
        secondaryTaskEnsurePath = client.newNamespaceAwareEnsurePath(secondaryTaskPath);
        secondaryTaskNodeFunc = nodeFuncs.getOrDefault(multiQueueInfo.getSecondaryTaskType().getMode(), nodeFuncs.get(TaskMode.STANDARD));
        secondaryTaskKeyFunc = keyFuncs.getOrDefault(multiQueueInfo.getSecondaryTaskType().getMode(), keyFuncs.get(TaskMode.STANDARD));

    }


    @Override
    public WorkflowManagerState.State getState() {
        return state.get();
    }

    @Override
    public void debugValidateClosed() {
        Preconditions.checkState(executorService.isTerminated());
    }

    public void start() {
        if (started.compareAndSet(false, true)) {
            executorService.submit(this::runLoop);
        }
    }

    @Override
    public void close() {
        if (started.compareAndSet(true, false)) {
            executorService.shutdownNow();
        }
    }

    private void runLoop() {
        log.info("Starting runLoop");

        try {
            while (started.get() && !Thread.currentThread().isInterrupted()) {
                state.set(WorkflowManagerState.State.SLEEPING);
                try {
                    primaryTaskEnsurePath.ensure(client.getZookeeperClient());
                    CountDownLatch countDownLatch = new CountDownLatch(1);
                    Watcher primaryTaskWatcher = event -> countDownLatch.countDown();
                    List<String> nodes = client.getChildren().usingWatcher(primaryTaskWatcher).forPath(primaryTaskPath);
                    if (nodes.size() == 0) {
                        Watcher secondaryTaskWatcher = event -> countDownLatch.countDown();
                        List<String> secondaryTaskNodes = client.getChildren().usingWatcher(secondaryTaskWatcher).forPath(secondaryTaskPath);
                        if (nodes.size() == 0 && secondaryTaskNodes.size() == 0)
                            countDownLatch.await();

                        if (secondaryTaskNodes.size() != 0) {
                            processNode(secondaryTaskNodes, countDownLatch, false);
                        }
                        //Wait until at least one task is present in either primaryTask queue or secondaryTask queue
                    } else {
                        processNode(nodes, countDownLatch, true);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    log.error("Could not process queue", e);
                }

            }
        } finally {
            log.info("Exiting runLoop");
            state.set(WorkflowManagerState.State.CLOSED);
        }
    }

    private void processNode(List<String> nodes, CountDownLatch latch, boolean isPrimaryTask) throws Exception {


        NodeFunc nodeFunc;
        String taskPath;
        String taskLockPath;
        boolean isIdempotent;
        if (isPrimaryTask) {
            nodeFunc = primaryTaskNodeFunc;
            taskPath = primaryTaskPath;
            taskLockPath = primaryTaskLockPath;
            isIdempotent = primaryTaskIsIdempotent;
        } else {
            nodeFunc = secondaryTaskNodeFunc;
            taskPath = secondaryTaskPath;
            taskLockPath = secondaryTaskLockPath;
            isIdempotent = secondaryTaskIsIdempotent;
        }

        NodeAndDelay nodeAndDelay = nodeFunc.getNode(nodes);
        if (nodeAndDelay.delay.isPresent()) {
            latch.await(nodeAndDelay.delay.get(), TimeUnit.MILLISECONDS);
        }
        if (nodeAndDelay.node.isPresent()) {
            state.set(WorkflowManagerState.State.PROCESSING);
            processNode(nodeAndDelay.node.get(), taskPath, taskLockPath, isIdempotent);
        }
    }

    private void processNode(String node, String taskPath, String taskLockPath, boolean isIdempotent) throws Exception {
        String lockNodePath = ZKPaths.makePath(taskLockPath, node);
        boolean lockCreated = false;
        try {
            try {
                if (isIdempotent) {
                    client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(lockNodePath);
                    lockCreated = true;
                }

                String itemPath = ZKPaths.makePath(taskPath, node);
                Stat stat = new Stat();
                byte[] bytes = client.getData().storingStatIn(stat).forPath(itemPath);
                ExecutableTask executableTask = serializer.deserialize(bytes, ExecutableTask.class);
                try {
                    if (!isIdempotent) {
                        client.delete().withVersion(stat.getVersion()).forPath(itemPath);
                    }
                    taskRunner.executeTask(executableTask);
                    if (isIdempotent) {
                        client.delete().guaranteed().forPath(itemPath);
                    }
                } catch (Throwable e) {
                    log.error("Exception processing task at: " + itemPath, e);
                }
            } catch (KeeperException.NodeExistsException ignore) {
                // another process got it
            }
        } catch (KeeperException.NoNodeException | KeeperException.BadVersionException ignore) {
            // another process got this node already - ignore
        } finally {
            if (lockCreated) {
                client.delete().guaranteed().forPath(lockNodePath);
            }
        }
    }

    private static String priorityToString(long priority) {
        // the padded hex val of the number prefixed with a 0 for negative numbers
        // and a 1 for positive (so that it sorts correctly)
        long l = priority & 0xFFFFFFFFL;
        return String.format("%s%08X", (priority >= 0) ? "1" : "0", l);
    }

    private static String epochToString(long epoch) {
        return SEPARATOR + String.format("%08X", epoch) + SEPARATOR;
    }

    private static long getEpoch(String itemNode) {
        int index2 = itemNode.lastIndexOf(SEPARATOR);
        int index1 = (index2 > 0) ? itemNode.lastIndexOf(SEPARATOR, index2 - 1) : -1;
        if ((index1 > 0) && (index2 > (index1 + 1))) {
            try {
                String epochStr = itemNode.substring(index1 + 1, index2);
                return Long.parseLong(epochStr, 16);
            } catch (NumberFormatException ignore) {
                // ignore
            }
        }
        return 0;
    }
}
