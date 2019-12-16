package mtymes.simplequeue.worker;

import javafixes.concurrency.Runner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.lang.Math.max;
import static javafixes.concurrency.Runner.runner;

// todo: test this
public abstract class GenericSingleTaskWorker<Task> {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    public abstract Optional<Task> pickNextTask() throws Exception;

    public abstract void updateHeartBeat(Task task);

    public abstract void executeTask(Task task) throws Exception;

    public abstract void handleExecutionFailure(Task task, Exception exception) throws Exception;


    private static final AtomicInteger workerIdCounter = new AtomicInteger(0);

    protected final String workerName;
    private final Duration waitDurationIfNoTaskAvailable;
    private final Duration heartBeatPeriod;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final AtomicReference<Task> taskInProgress = new AtomicReference<>(null);

    private Runner runner;

    protected GenericSingleTaskWorker(Duration waitDurationIfNoTaskAvailable, Duration heartBeatPeriod) {
        this.workerName = "Worker" + "-" + workerIdCounter.incrementAndGet();

        this.waitDurationIfNoTaskAvailable = waitDurationIfNoTaskAvailable;
        this.heartBeatPeriod = heartBeatPeriod;
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
                Thread.sleep(heartBeatPeriod.toMillis());
            }
        });
    }

    private void registerWorker(Runner runner) {
        runner.run(shutdownInfo -> {
            while (!shutdownInfo.wasShutdownTriggered()) {

                try {

                    Optional<Task> optionalTask = Optional.empty();
                    try {
                        optionalTask = pickNextTask();
                    } catch (InterruptedException e) {
                        throw e;
                    } catch (Exception e) {
                        logger.error(workerName + ": Failed to pick next task " + optionalTask, e);
                    }

                    if (!optionalTask.isPresent()) {
                        logger.info(workerName + ": No available task found");
                    } else {
                        logger.info(workerName + ": Going to process next available task " + optionalTask);

                        Task task = optionalTask.get();
                        taskInProgress.set(task);
                        try {
                            executeTask(task);
                        } catch (InterruptedException e) {
                            throw e;
                        } catch (Exception failure) {
                            logger.error(workerName + ": Failed to execute task " + optionalTask, failure);
                            try {
                                handleExecutionFailure(task, failure);
                            } catch (InterruptedException e) {
                                throw e;
                            } catch (Exception e) {
                                logger.error(workerName + ": Failed to handle task failure for task " + optionalTask, e);
                            }
                        } finally {
                            taskInProgress.set(null);
                        }
                    }

                    // this allows to recognize InterruptedException in case there would be no wait on unavailable task
                    Thread.sleep((optionalTask.isPresent())
                            ? 10
                            : max(waitDurationIfNoTaskAvailable.toMillis(), 10)
                    );

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
