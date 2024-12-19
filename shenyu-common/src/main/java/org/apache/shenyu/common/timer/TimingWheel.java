/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.shenyu.common.timer;

import java.util.Objects;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * TimingWheel .
 * This is a Hierarchical wheel timer implementation.
 */
class TimingWheel {

    private final Long tickMs;

    private final Integer wheelSize;

    private final AtomicInteger taskCounter;

    private final DelayQueue<TimerTaskList> queue;

    private final Long interval;

    private final TimerTaskList[] buckets;

    private Long currentTime;

    private TimingWheel overflowWheel;

    /**
     * Instantiates a new Timing wheel.
     *
     * @param tickMs      the tick ms
     * @param wheelSize   the wheel size
     * @param startMs     the start ms
     * @param taskCounter the task counter
     * @param queue       the queue
     */
    TimingWheel(final Long tickMs, final Integer wheelSize, final Long startMs, final AtomicInteger taskCounter, final DelayQueue<TimerTaskList> queue) {
        this.tickMs = tickMs;
        this.wheelSize = wheelSize;
        this.taskCounter = taskCounter;
        this.queue = queue;
        this.interval = tickMs * wheelSize;
        this.currentTime = startMs - (startMs % tickMs);
        this.buckets = new TimerTaskList[wheelSize];
    }

    private synchronized void addOverflowWheel() {
        if (overflowWheel == null) {
            overflowWheel = new TimingWheel(interval, wheelSize, currentTime, taskCounter, queue);
        }
    }

    /**
     * Add boolean.
     *
     * @param taskEntry the task entry
     * @return the boolean
     */
    boolean add(final TimerTaskList.TimerTaskEntry taskEntry) {
        // 获取任务的超时时间
        Long expirationMs = taskEntry.getExpirationMs();
        // 如果任务已经取消，则返回false 放弃执行
        if (taskEntry.cancelled()) {
            return false;
        }
        // 判断任务是否已经过期 如果已经过期则返回false 立即执行
        if (expirationMs < currentTime + tickMs) {
            return false;
        }

        // 如果任务的超时时间小于当前时间加上一个时间间隔，则将任务放入当前时间轮的对应槽中
        if (expirationMs < currentTime + interval) {
            //Put in its own bucket
            // 计算任务的超时时间对应的槽的位置，将任务放入对应的槽中 并设置槽的过期时间
            // 最后将任务放入延迟队列中
            long virtualId = expirationMs / tickMs;
            int index = (int) (virtualId % wheelSize);
            TimerTaskList bucket = this.getBucket(index);
            bucket.add(taskEntry);
            if (bucket.setExpiration(virtualId * tickMs)) {
                queue.offer(bucket);
            }
            return true;
        }

        // 如果任务的超时时间大于当前时间加上一个时间间隔，则将任务放入下一个时间轮的对应槽中
        if (Objects.isNull(overflowWheel)) {
            // 创建下一个时间轮
            addOverflowWheel();
        }
        // 将任务放入高一级时间轮的对应槽中
        return overflowWheel.add(taskEntry);
    }

    /**
     * Advance clock.
     * 推进时间轮的时间
     * @param timeMs the time ms
     */
    void advanceClock(final long timeMs) {
        if (timeMs >= currentTime + tickMs) {
            currentTime = timeMs - (timeMs % tickMs);
        }
        if (Objects.nonNull(overflowWheel)) {
            overflowWheel.advanceClock(currentTime);
        }
    }

    private TimerTaskList getBucket(final int index) {
        TimerTaskList bucket = buckets[index];
        if (Objects.isNull(bucket)) {
            synchronized (this) {
                bucket = buckets[index];
                if (Objects.isNull(bucket)) {
                    bucket = new TimerTaskList(taskCounter);
                    buckets[index] = bucket;
                }
            }
        }
        return bucket;
    }

}
