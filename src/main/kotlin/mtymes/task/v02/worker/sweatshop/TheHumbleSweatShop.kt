package mtymes.task.v02.worker.sweatshop

import javafixes.concurrency.Runner
import mtymes.task.v02.scheduler.domain.WorkerId
import mtymes.task.v02.worker.HeartBeatingTaskWorker
import mtymes.task.v02.worker.TaskWorker
import org.apache.commons.lang3.StringUtils.isBlank
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.*
import java.util.UUID.randomUUID
import java.util.concurrent.Future
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference


// todo: mtymes - implement in better way
// todo: mtymes - test
class TheHumbleSweatShop : SweatShop {

    companion object {
        val logger = LoggerFactory.getLogger(TheHumbleSweatShop::class.java) as Logger
    }

    data class WorkContext<T>(
        val workerId: WorkerId,
        val runner: Runner,
        val taskInProgress: AtomicReference<T> = AtomicReference(null),

        val shutDownGracefully: AtomicBoolean = AtomicBoolean(false),

        val hasHeartBeatSupport: Boolean,
        val lastHeartBeaterId: AtomicReference<UUID> = AtomicReference(null),
        val lastHeartBeater: AtomicReference<Future<*>> = AtomicReference(null)
    )

    val isShutDown: AtomicBoolean = AtomicBoolean(false)
    val workers: MutableMap<WorkerId, WorkContext<*>> = mutableMapOf()

    override fun <T> addAndStartWorker(
        worker: TaskWorker<T>,
        workerId: WorkerId
    ) {
        synchronized(workers) {
            assertIsNotClosed()

            if (workers.containsKey(workerId)) {
                throw IllegalArgumentException("WorkerId '${workerId}' already used for a different registered worker")
            }

            val hasHeartBeatSupport = worker is HeartBeatingTaskWorker<*>
            val numberOfThreads =
                if (hasHeartBeatSupport) 2
                else 1
            val runner = Runner.runner(numberOfThreads)

            val context: WorkContext<T> = WorkContext(
                workerId = workerId,
                runner = runner,
                hasHeartBeatSupport = hasHeartBeatSupport
            )
            workers.put(workerId, context)

            startWorker(
                workContext = context,
                worker = worker
            )
        }
    }

    override fun stopAndRemoveWorker(
        workerId: WorkerId,
        stopGracefully: Boolean
    ): Boolean {
        synchronized(workers) {
            val context = workers.remove(workerId)
            if (context == null) {
                return false
            } else {
                if (stopGracefully) {
                    context.shutDownGracefully.set(true)
                } else {
                    context.runner.shutdownNow()
                }
                return true
            }
        }
    }

    override fun close() {
        synchronized(workers) {
            if (isShutDown.get()) {
                return
            }

            val workerIds = workers.keys.toList()
            for (workerId in workerIds) {
                runAndIgnoreExceptions {
                    stopAndRemoveWorker(
                        workerId = workerId,
                        stopGracefully = false
                    )
                }
            }

            isShutDown.set(true)
        }
    }

    private fun assertIsNotClosed() {
        if (isShutDown.get()) {
            throw IllegalStateException("SweatShop is already closed")
        }
    }


    private fun <T> startWorker(
        workContext: WorkContext<T>,
        worker: TaskWorker<T>
    ): Future<Void>? = workContext.runner.run { shutdownInfo ->

        val workerId = workContext.workerId

        var taskNotFoundNTimesInARow = 0L
        while (!shutdownInfo.wasShutdownTriggered() && !workContext.shutDownGracefully.get()) {

            try {

                // FETCH TASK

                var task: T? = null
                try {
                    task = worker.fetchNextTaskToProcess(workerId)
                } catch (e: InterruptedException) {
                    throw e
                } catch (e: Exception) {
                    runAndIgnoreExceptions {
                        logger.error("[${workerId}]: Failed to fetch next task", e)
                    }
                }


                // PROCESS TASK

                workContext.taskInProgress.set(task)

                if (task == null) {
                    taskNotFoundNTimesInARow += 1

                    runAndIgnoreExceptions {
                        logger.debug("[${workerId}]: No available task found")
                    }

                } else {
                    taskNotFoundNTimesInARow = 0

                    runAndIgnoreExceptions {
                        val taskString: String? = taskToLoggableString(worker, task, workerId)
                        if (isBlank(taskString)) {
                            logger.info("[${workerId}]: Going to process next available task")
                        } else {
                            logger.info("[${workerId}]: Going to process next available task '${taskString}'")
                        }
                    }

                    try {
                        if (workContext.hasHeartBeatSupport) {
                            startHeartBeater(
                                workContext = workContext,
                                worker = worker,
                                task = task
                            )
                        }

                        worker.executeTask(
                            task = task,
                            workerId = workerId
                        )

                        runAndIgnoreExceptions {
                            val taskString: String? = taskToLoggableString(worker, task, workerId)
                            if (isBlank(taskString)) {
                                logger.info("[${workerId}]: Finished processing task")
                            } else {
                                logger.info("[${workerId}]: Finished processing task '${taskString}'")
                            }
                        }
                    } catch (e: InterruptedException) {
                        throw e
                    } catch (e: Exception) {
                        runAndIgnoreExceptions {
                            val taskString: String? = taskToLoggableString(worker, task, workerId)
                            if (isBlank(taskString)) {
                                logger.warn("[${workerId}]: Failed to execute task", e)
                            } else {
                                logger.warn("[${workerId}]: Failed to execute task '${taskString}'", e)
                            }
                        }

                        try {
                            worker.handleExecutionFailure(
                                task,
                                workerId,
                                e
                            )
                        } catch (e: InterruptedException) {
                            throw e
                        } catch (e: Exception) {
                            runAndIgnoreExceptions {
                                val taskString: String? = taskToLoggableString(worker, task, workerId)
                                if (isBlank(taskString)) {
                                    logger.error("[${workerId}]: Failed to handle failure of task", e)
                                } else {
                                    logger.error("[${workerId}]: Failed to handle failure of task '${taskString}'", e)
                                }
                            }
                        }
                    } finally {
                        workContext.taskInProgress.set(null)

                        if (workContext.hasHeartBeatSupport) {
                            stopAndRemoveHeartBeater(workContext)
                        }
                    }
                }


                // SLEEP DELAY BEFORE FETCHING NEXT TASK

                if (task == null) {

                    // default to 1 minute if fails to get the sleep duration
                    var sleepDuration: Duration = Duration.ofMinutes(1)
                    try {
                        sleepDuration = worker.sleepDurationIfNoTaskWasAvailable(
                            taskNotFoundNTimesInARow,
                            workerId
                        )
                    } catch (e: Exception) {
                        runAndIgnoreExceptions {
                            logger.error("${workerId}]: Failed to evaluate sleep duration if no task was available", e)
                        }
                    }
                    Thread.sleep(sleepDuration.toMillis())

                } else {

                    // this allows to recognize InterruptedException in case there would be no wait on unavailable task
                    Thread.sleep(1)

                }

            } catch (e: InterruptedException) {
                runAndIgnoreExceptions {
                    val isExpected = shutdownInfo.wasShutdownTriggered()
                    if (isExpected) {
                        logger.info("${workerId}]: Worker thread has been interrupted")
                    } else {
                        logger.error("${workerId}]: Worker thread has been interrupted", e)
                    }
                }
                break
            } catch (e: Exception) {
                runAndIgnoreExceptions {
                    logger.error("${workerId}]: Unexpected failure", e)
                }
            }
        }

        runAndIgnoreExceptions {
            logger.info("[${workerId}]: Worker thread has been shut down")
        }

        if (workContext.shutDownGracefully.get()) {
            runAndIgnoreExceptions {
                workContext.runner.shutdownNow()
            }
        }
    }

    private fun <T> startHeartBeater(
        workContext: WorkContext<T>,
        worker: TaskWorker<T>,
        task: T
    ) {
        val workerId = workContext.workerId

        val heartBeaterId = randomUUID()
        workContext.lastHeartBeaterId.set(heartBeaterId)

        val heartBeatingTaskWorker = worker as HeartBeatingTaskWorker<T>
        // we should fail and don't start task processing if we're unable to get the heart beat interval
        val heartBeatInterval = heartBeatingTaskWorker.heartBeatInterval(task, workerId)
        val heartBeater: Future<Void> = workContext.runner.run { shutdownInfo ->
            while (
                !shutdownInfo.wasShutdownTriggered() &&
                heartBeaterId.equals(workContext.lastHeartBeaterId.get())
            ) {
                try {
                    Thread.sleep(heartBeatInterval.toMillis())

                    heartBeatingTaskWorker.updateHeartBeat(
                        task = task,
                        workerId = workerId
                    )
                } catch (e: InterruptedException) {
                    runAndIgnoreExceptions {
                        logger.info("[${workerId}]: Heart beat thread has been interrupted")
                    }
                    break
                } catch (e: Exception) {
                    runAndIgnoreExceptions {
                        val taskString: String? = taskToLoggableString(worker, task, workerId)
                        if (isBlank(taskString)) {
                            logger.error("[${workerId}]: Failed to update heart beat for task", e)
                        } else {
                            logger.error("[${workerId}]: Failed to update heart beat for task '${taskString}'", e)
                        }
                    }
                }
            }
        }
        workContext.lastHeartBeater.set(heartBeater)
    }

    private fun <T> stopAndRemoveHeartBeater(
        workContext: WorkContext<T>
    ) {
        workContext.lastHeartBeaterId.set(null)
        runAndIgnoreExceptions {
            workContext.lastHeartBeater.get()?.cancel(true)
        }
        workContext.lastHeartBeater.set(null)
    }

    private fun <T> taskToLoggableString(
        worker: TaskWorker<T>,
        task: T,
        workerId: WorkerId
    ): String? {
        var taskString: String? = null
        try {
            taskString = worker.taskToLoggableString(
                task,
                workerId
            )
        } catch (e: Exception) {
            // ignore
        }
        return taskString
    }

    private fun runAndIgnoreExceptions(code: () -> Unit) {
        try {
            code.invoke()
        } catch (e: Exception) {
            // ignore
        }
    }
}