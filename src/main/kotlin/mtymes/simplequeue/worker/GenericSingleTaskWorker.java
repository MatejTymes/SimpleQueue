package mtymes.simplequeue.worker;

import javafixes.concurrency.Runner;
import javafixes.object.Tuple;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static javafixes.concurrency.Runner.runner;

// todo: test this
public abstract class GenericSingleTaskWorker<Task> {

    protected abstract Optional<Tuple<UUID, Task>> pickNextTask();

    protected abstract void updateHeartBeat(UUID taskId);

    protected abstract void executeTask(UUID taskId, Task task);

    protected abstract void handleSuccessfulExecution(UUID taskId, Task task);

    protected abstract void handleFailedExecution(UUID taskId, Task task, Exception exception);


    private final Duration waitDurationIfNoTaskAvailable;
    private final Duration heartBeadPeriod;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final AtomicReference<UUID> inProgressTaskId = new AtomicReference<>(null);

    private Runner runner;

    protected GenericSingleTaskWorker(Duration waitDurationIfNoTaskAvailable, Duration heartBeadPeriod) {
        this.waitDurationIfNoTaskAvailable = waitDurationIfNoTaskAvailable;
        this.heartBeadPeriod = heartBeadPeriod;
    }

    public void start() {
        synchronized (isRunning) {
            if (isRunning.get()) {
                throw new IllegalStateException(this.getClass().getSimpleName() + " is already running");
            }
            runner = runner(2);
            registerHeartBeater(runner);
            registerWorker(runner);
            isRunning.set(true);
        }
    }

    public void stop() {
        synchronized (isRunning) {
            if (!isRunning.get()) {
                throw new IllegalStateException(this.getClass().getSimpleName() + " is not running");
            }
            runner.shutdownAndAwaitTermination();
            isRunning.set(false);
        }
    }

    public boolean isRunning() {
        return isRunning.get();
    }

    private void registerHeartBeater(Runner runner) {
        runner.run(shutdownInfo -> {
            while (!shutdownInfo.wasShutdownTriggered()) {
                UUID taskId = inProgressTaskId.get();
                if (taskId != null) {
                    try {
                        updateHeartBeat(taskId);
                    } catch (Exception e) {
                        // todo: log error
                        e.printStackTrace();
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
                    Optional<Tuple<UUID, Task>> taskTuple = pickNextTask();
                    if (!taskTuple.isPresent()) {
                        Thread.sleep(Math.min(10, waitDurationIfNoTaskAvailable.toMillis())); // this also allows to stop the handler
                    } else {
                        try {
                            UUID taskId = taskTuple.get().a();
                            inProgressTaskId.set(taskId);

                            Task task = taskTuple.get().b();
                            try {
                                executeTask(taskId, task);
                                handleSuccessfulExecution(taskId, task);
                            } catch (Exception e) {
                                // todo: log error
                                e.printStackTrace();
                                handleFailedExecution(taskId, task, e);
                            }
                        } catch (Exception e) {
                            inProgressTaskId.set(null);
                            // todo: log error
                            e.printStackTrace();
                        }

                        Thread.sleep(10); // this allows to stop the handler
                    }
                } catch (InterruptedException e) {
                    throw e; // rethrow exception
                } catch (Exception e) {
                    // todo: log error
                    e.printStackTrace();
                }
            }
        });
    }
}
