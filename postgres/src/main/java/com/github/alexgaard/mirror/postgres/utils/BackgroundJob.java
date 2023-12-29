package com.github.alexgaard.mirror.postgres.utils;

import com.github.alexgaard.mirror.core.utils.ExceptionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class BackgroundJob {

    private final static Logger log = LoggerFactory.getLogger(BackgroundJob.class);

    private final String name;

    private final long intervalMs;

    private final long backoffIncreaseMs;

    private final long maxBackoffMs;

    private long currentBackoffMs;

    private Thread job;

    public BackgroundJob(String name, Duration interval, Duration backoffIncrease, Duration maxBackoff) {
        this.name = name;
        this.intervalMs = interval.toMillis();
        this.backoffIncreaseMs = backoffIncrease.toMillis();
        this.maxBackoffMs = maxBackoff.toMillis();
    }

    public boolean isRunning() {
        return job != null && job.isAlive();
    }

    public synchronized void start(ExceptionUtil.UnsafeRunnable unsafeRunnable) {
        stop();

        job = new Thread(() -> {
            while (!Thread.interrupted()) {
                try {
                    unsafeRunnable.run();

                    currentBackoffMs = intervalMs;
                } catch (InterruptedException e) {
                    return;
                } catch (Exception e) {
                    log.error("Caught exception from background job", e);
                    currentBackoffMs = Math.min(maxBackoffMs, currentBackoffMs + backoffIncreaseMs);
                }

                try {
                    Thread.sleep(currentBackoffMs);
                } catch (InterruptedException e) {
                    return;
                }
            }
        });

        job.setName(name);
        job.setDaemon(false);
        job.start();
    }

    public synchronized void stop() {
        if (job != null && job.isAlive()) {
            job.interrupt();
        }
    }

}
