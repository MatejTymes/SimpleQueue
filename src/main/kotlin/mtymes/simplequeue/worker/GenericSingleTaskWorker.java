package mtymes.simplequeue.worker;

import javafixes.concurrency.Runner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static javafixes.concurrency.Runner.runner;

// todo: test this
public abstract class GenericSingleTaskWorker<Task> {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    protected abstract Optional<Task> pickNextTask();

    protected abstract void updateHeartBeat(Task task);

    protected abstract void executeTask(Task task);

    protected abstract void handleExecutionFailure(Task task, Exception exception);


    private final Duration waitDurationIfNoTaskAvailable;
    private final Duration heartBeadPeriod;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final AtomicReference<Task> taskInProgress = new AtomicReference<>(null);

    private Runner runner;

    protected GenericSingleTaskWorker(Duration waitDurationIfNoTaskAvailable, Duration heartBeadPeriod) {
        this.waitDurationIfNoTaskAvailable = waitDurationIfNoTaskAvailable;
        this.heartBeadPeriod = heartBeadPeriod;
    }

    public void start() {
        synchronized (isRunning) {
            logger.info("Starting task worker");
            if (isRunning.get()) {
                logger.error("Task worker WAS already running");
                throw new IllegalStateException(this.getClass().getSimpleName() + " is already running");
            }
            runner = runner(2);
            registerHeartBeater(runner);
            registerWorker(runner);
            isRunning.set(true);

            logger.info("Task worker started");
        }
    }

    public void stop() {
        synchronized (isRunning) {
            logger.info("Stopping task worker");

            if (!isRunning.get()) {
                logger.error("Task worker WAS NOT running");
                throw new IllegalStateException(this.getClass().getSimpleName() + " is not running");
            }
            runner.shutdownNow().waitTillDone();
            isRunning.set(false);

            logger.info("Task worker stopped");
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
                        logger.error("Failed to update hear beat", e);
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
                        logger.info("No available task found");
                        Thread.sleep(Math.min(10, waitDurationIfNoTaskAvailable.toMillis())); // this also allows to stop the handler
                    } else {
                        logger.info("Going to process next available task " + optionalTask);
                        try {
                            Task task = optionalTask.get();
                            taskInProgress.set(task);

                            try {
                                executeTask(task);
                            } catch (Exception e) {
                                logger.error("Failed to execute task " + optionalTask, e);
                                e.printStackTrace();
                                handleExecutionFailure(task, e);
                            }
                        } catch (Exception e) {
                            taskInProgress.set(null);
                            logger.error("Failed to execute task " + optionalTask, e);
                        }

                        Thread.sleep(10); // this allows to stop the handler
                    }
                } catch (InterruptedException e) {
                    throw e; // rethrow exception
                } catch (Exception e) {
                    logger.error("Failed to process next available task ", e);
                }
            }
        });
    }
}
