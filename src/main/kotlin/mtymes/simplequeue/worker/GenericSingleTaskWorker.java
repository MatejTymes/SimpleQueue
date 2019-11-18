package mtymes.simplequeue.worker;

import javafixes.concurrency.Runner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static javafixes.concurrency.Runner.runner;

// todo: test this
public abstract class GenericSingleTaskWorker<Task> {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    protected abstract Optional<Task> pickNextTask();

    protected abstract void updateHeartBeat(Task task);

    protected abstract void executeTask(Task task) throws Exception;

    protected abstract void handleExecutionFailure(Task task, Exception exception) throws Exception;


    private static final AtomicInteger workerIdCounter = new AtomicInteger(0);

    protected final String workerName;
    private final Duration waitDurationIfNoTaskAvailable;
    private final Duration heartBeadPeriod;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final AtomicReference<Task> taskInProgress = new AtomicReference<>(null);

    private Runner runner;

    protected GenericSingleTaskWorker(Duration waitDurationIfNoTaskAvailable, Duration heartBeadPeriod) {
        this.workerName = "Worker" + "-" + workerIdCounter.incrementAndGet();

        this.waitDurationIfNoTaskAvailable = waitDurationIfNoTaskAvailable;
        this.heartBeadPeriod = heartBeadPeriod;
    }

    public void start() {
        synchronized (isRunning) {
            logger.info(workerName + ": Starting task worker");
            if (isRunning.get()) {
                logger.error(workerName + ": Task worker WAS already running");
                throw new IllegalStateException(this.getClass().getSimpleName() + " is already running");
            }
            runner = runner(2);
            registerHeartBeater(runner);
            registerWorker(runner);
            isRunning.set(true);

            logger.info(workerName + ": Task worker started");
        }
    }

    public void stop() {
        synchronized (isRunning) {
            logger.info(workerName + ": Stopping task worker");

            if (!isRunning.get()) {
                logger.error(workerName + ": Task worker WAS NOT running");
                throw new IllegalStateException(this.getClass().getSimpleName() + " is not running");
            }
            runner.shutdownNow().waitTillDone();
            isRunning.set(false);

            logger.info(workerName + ": Task worker stopped");
        }
    }

    public boolean isRunning() {
        return isRunning.get();
    }

    private void registerHeartBeater(Runner runner) {
        runner.run(shutdownInfo -> {
            while (!shutdownInfo.wasShutdownTriggered()) {
                Task task = taskInProgress.get();
                if (task != null) {
                    try {
                        updateHeartBeat(task);
                    } catch (Exception e) {
                        logger.error(workerName + ": Failed to update hear beat", e);
                    }
                }
                Thread.sleep(heartBeadPeriod.toMillis());
            }
        });
    }

    private void registerWorker(Runner runner) {
        runner.run(shutdownInfo -> {
            while (!shutdownInfo.wasShutdownTriggered()) {
                try {
                    Optional<Task> optionalTask = pickNextTask();
                    if (!optionalTask.isPresent()) {
                        logger.info(workerName + ": No available task found");
                        Thread.sleep(Math.min(
                                waitDurationIfNoTaskAvailable.toMillis(),
                                10 // this allows to recognize InterruptedException in case there would be no wait on unavailable task
                        ));
                    } else {
                        logger.info(workerName + ": Going to process next available task " + optionalTask);
                        try {
                            Task task = optionalTask.get();
                            taskInProgress.set(task);

                            try {
                                executeTask(task);
                            } catch (Exception e) {
                                logger.error(workerName + ": Failed to execute task " + optionalTask, e);
                                handleExecutionFailure(task, e);
                            }
                        } catch (Exception e) {
                            logger.error(workerName + ": Failed to execute task " + optionalTask, e);
                        } finally {
                            taskInProgress.set(null);
                        }

                        Thread.sleep(10); // this allows to recognize InterruptedException
                    }
                } catch (InterruptedException e) {
                    logger.warn(workerName + ": Worker thread has been interrupted ", e);
                    throw e; // rethrow exception
                } catch (Exception e) {
                    logger.error(workerName + ": Failed to process next available task ", e);
                }
            }

            logger.warn(workerName + ": Worker thread has been shut down");
        });
    }
}
