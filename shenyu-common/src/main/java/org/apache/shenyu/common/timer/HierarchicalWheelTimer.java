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
import org.apache.shenyu.common.concurrent.ShenyuThreadFactory;

import java.util.concurrent.DelayQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * HierarchicalWheelTimer
 * The type Hierarchical Wheel timer.
 *
 *  分层时间轮 基于延迟队列实现  用于解决任务重试的问题
 * @see TimingWheel
 */
public class HierarchicalWheelTimer implements Timer {

    private static final AtomicIntegerFieldUpdater<HierarchicalWheelTimer> WORKER_STATE_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(HierarchicalWheelTimer.class, "workerState");

    private final ExecutorService taskExecutor;

    private final DelayQueue<TimerTaskList> delayQueue = new DelayQueue<>();

    private final AtomicInteger taskCounter = new AtomicInteger(0);

    // 时间轮
    private final TimingWheel timingWheel;

    private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    private final ReentrantReadWriteLock.ReadLock readLock = readWriteLock.readLock();

    private final ReentrantReadWriteLock.WriteLock writeLock = readWriteLock.writeLock();

    private volatile int workerState;

    private final Thread workerThread;

    /**
     * Instantiates a new System timer.
     *
     * @param executorName the executor name
     */
    public HierarchicalWheelTimer(final String executorName) {
        this(executorName, 1L, 20, TimeUnit.NANOSECONDS.toMillis(System.nanoTime()));
    }

    /**
     * Instantiates a new System timer.
     *
     * @param executorName the executor name
     * @param tickMs       the tick ms
     * @param wheelSize    the wheel size
     * @param startMs      the start ms
     */
    public HierarchicalWheelTimer(final String executorName,
                                  final Long tickMs,
                                  final Integer wheelSize,
                                  final Long startMs) {
        ThreadFactory threadFactory = ShenyuThreadFactory.create(executorName, false);
        taskExecutor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(), threadFactory);
        workerThread = threadFactory.newThread(new Worker(this));
        timingWheel = new TimingWheel(tickMs, wheelSize, startMs, taskCounter, delayQueue);
    }

    // 添加任务到时间轮中
    @Override
    public void add(final TimerTask timerTask) {
        if (Objects.isNull(timerTask)) {
            throw new NullPointerException("timer task null");
        }
        this.readLock.lock();
        try {
            start();
            long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
            this.addTimerTaskEntry(new TimerTaskList.TimerTaskEntry(this, timerTask, timerTask.getDelayMs() + millis));
        } finally {
            this.readLock.unlock();
        }

    }

    private void addTimerTaskEntry(final TimerTaskList.TimerTaskEntry timerTaskEntry) {
        // 将任务添加到时间轮中 如果添加失败则直接执行任务
        if (!timingWheel.add(timerTaskEntry)) {
            // 如果任务已经取消，则直接返回
            if (!timerTaskEntry.cancelled()) {
                // 提交任务到线程池中执行
                taskExecutor.submit(() -> timerTaskEntry.getTimerTask().run(timerTaskEntry));
            }
        }
    }

    @Override
    public void advanceClock(final long timeoutMs) throws InterruptedException {
        // 从延迟队列中获取到期的任务 如果没有到期的任务则阻塞等待 timeoutMs 毫秒
        // 如果有到期的任务则先推进时间轮的时间，然后将到期的任务提交到线程池中执行 addTimerTaskEntry
        TimerTaskList bucket = delayQueue.poll(timeoutMs, TimeUnit.MILLISECONDS);
        if (Objects.nonNull(bucket)) {
            writeLock.lock();
            try {
                while (Objects.nonNull(bucket)) {
                    // 推进时间轮的时间
                    timingWheel.advanceClock(bucket.getExpiration());
                    // 将到期的任务提交到线程池中执行
                    bucket.flush(this::addTimerTaskEntry);
                    bucket = delayQueue.poll();
                }
            } finally {
                writeLock.unlock();
            }
        }
    }

    // 启动时间轮 通过CAS操作保证只有一个线程启动时间轮
    private void start() {
        int state = WORKER_STATE_UPDATER.get(this);
        if (state == 0) {
            if (WORKER_STATE_UPDATER.compareAndSet(this, 0, 1)) {
                workerThread.start();
            }
        }
    }

    @Override
    public int size() {
        return taskCounter.get();
    }

    @Override
    public void shutdown() {
        taskExecutor.shutdown();
    }

    private static class Worker implements Runnable {

        private final Timer timer;

        /**
         * Instantiates a new Worker.
         *
         * @param timer the timer
         */
        Worker(final Timer timer) {
            this.timer = timer;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    timer.advanceClock(100L);
                } catch (InterruptedException ignored) {
                }
            }
        }
    }
}
