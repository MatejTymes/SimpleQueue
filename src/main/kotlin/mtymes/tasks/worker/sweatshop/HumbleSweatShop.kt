package mtymes.tasks.worker.sweatshop

import javafixes.concurrency.Runner
import mtymes.tasks.common.domain.WorkerId
import mtymes.tasks.common.exception.ExceptionUtil.runAndIgnoreExceptions
import mtymes.tasks.common.time.Durations.ONE_MINUTE
import mtymes.tasks.worker.HeartBeatingWorker
import mtymes.tasks.worker.Worker
import mtymes.tasks.worker.sweatshop.ShutDownMode.OnceNoMoreWork
import mtymes.tasks.worker.sweatshop.UpdateOutcome.*
import org.apache.commons.lang3.StringUtils.isBlank
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.*
import java.util.UUID.randomUUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference


class HumbleSweatShop : SweatShop {

    companion object {
        val logger = LoggerFactory.getLogger(HumbleSweatShop::class.java) as Logger
    }

    data class WorkContext<T>(
        val workerId: WorkerId,
        val worker: Worker<T>,
        val runner: Runner,
        val taskInProgress: AtomicReference<T> = AtomicReference(null),

        val shutDownMode: AtomicReference<ShutDownMode> = AtomicReference(null),
        val waitForNextTaskCountDownLatch: CountDownLatch = CountDownLatch(1),

        val hasHeartBeatSupport: Boolean,
        val lastHeartBeaterId: AtomicReference<UUID> = AtomicReference(null),
        val lastHeartBeater: AtomicReference<Future<*>> = AtomicReference(null)
    ) {
        val logId: String = worker.workerLogId(workerId)
        val hasNeverEndingStreamOfWork = worker.hasNeverEndingStreamOfWork()


        fun applyShutDownMode(suggestedMode: ShutDownMode): UpdateOutcome {
            val outcome: UpdateOutcome

            val modeToApply = suggestedMode.modeToUseIf(hasNeverEndingStreamOfWork)

            synchronized(shutDownMode) {
                val currentMode = shutDownMode.get()
                if (currentMode == null || currentMode.priority < modeToApply.priority) {
                    shutDownMode.set(modeToApply)
                    if (modeToApply.priority >= ShutDownMode.OnceCurrentTaskIsFinished.priority) {
                        waitForNextTaskCountDownLatch.countDown()
                    }
                    runAndIgnoreExceptions {
                        logger.info("[${logId}]: Applied ShutDownMode '${modeToApply}'")
                    }
                    outcome = WasApplied
                } else if (currentMode.priority == modeToApply.priority) {
                    outcome = WasAlreadyInWantedState
                } else {
                    outcome = WasNotApplied
                }
            }

            return outcome
        }

        fun canWorkOnNextTask(): Boolean {
            val currentShutDownMode = shutDownMode.get()
            return currentShutDownMode == null || currentShutDownMode.priority < ShutDownMode.OnceCurrentTaskIsFinished.priority
        }

        fun endIfNoWorkFound(): Boolean {
            return shutDownMode.get() == OnceNoMoreWork
        }

        fun isBeingGracefullyShutDown(): Boolean {
            return shutDownMode.get()?.isGraceful ?: false
        }

        fun sleepIfWillAskForNextTask(
            sleepDuration: Duration
        ) {
            waitForNextTaskCountDownLatch.await(sleepDuration.toMillis(), TimeUnit.MILLISECONDS)
        }
    }

    private val isClosed: AtomicBoolean = AtomicBoolean(false)
    private val workers: MutableMap<WorkerId, WorkContext<*>> = mutableMapOf()

    override fun <T> addAndStartWorker(
        worker: Worker<T>,
        workerId: WorkerId
    ): WorkerId {
        synchronized(workers) {
            if (isClosed.get()) {
                throw IllegalStateException("${javaClass.simpleName} is closed")
            }

            if (workers.containsKey(workerId)) {
                throw IllegalArgumentException("WorkerId '${workerId}' already used for a different registered worker")
            }

            val hasHeartBeatSupport = worker is HeartBeatingWorker<*>
            val numberOfThreads =
                if (hasHeartBeatSupport) 2
                else 1
            val runner = Runner.runner(numberOfThreads)

            runAndIgnoreExceptions {
                logger.info("${this.javaClass.simpleName} registered worker with WorkerId '${workerId}' of type '${worker.javaClass.simpleName}'")
            }
            val context: WorkContext<T> = WorkContext(
                workerId = workerId,
                worker = worker,
                runner = runner,
                hasHeartBeatSupport = hasHeartBeatSupport
            )
            workers.put(workerId, context)

            startWorker(
                workContext = context,
                worker = worker
            )

            return workerId
        }
    }

    override fun stopWorker(
        workerId: WorkerId,
        shutDownMode: ShutDownMode
    ): UpdateOutcome {
        synchronized(workers) {
            if (shutDownMode.isGraceful) {
                val context = workers.get(workerId)
                if (context == null) {
                    // worker not recognized
                    return WasNotApplied
                } else {
                    return context.applyShutDownMode(shutDownMode)
                }
            } else {
                val context = workers.remove(workerId)
                if (context == null) {
                    // worker not recognized
                    return WasNotApplied
                } else {
                    context.applyShutDownMode(shutDownMode)
                    context.runner.shutdownNow()
                    return WasApplied
                }
            }
        }
    }

    override fun workerSummaries(): List<WorkerSummary> {
        synchronized(workers) {
            return workers.values.map { context ->
                WorkerSummary(
                    workerId = context.workerId,
                    worker = context.worker,
                    isWorking = context.taskInProgress.get() != null,
                    hasNeverEndingStreamOfWork = context.hasNeverEndingStreamOfWork,
                    whenShouldStop = context.shutDownMode.get()
                )
            }
        }
    }

    override fun stopAllWorkers(
        shutDownMode: ShutDownMode,
        waitTillDone: Boolean
    ) {
        val allRunners = mutableListOf<Runner>()

        runAndIgnoreExceptions {
            logger.info("${this.javaClass.simpleName} will try to stop All Workers using ShutDownMode '${shutDownMode}'")
        }

        synchronized(workers) {
            if (workers.isNotEmpty()) {
                val workerIds = workers.keys.toList()
                for (workerId in workerIds) {
                    if (waitTillDone) {
                        runAndIgnoreExceptions {
                            workers.get(workerId)?.also {
                                allRunners.add(it.runner)
                            }
                        }
                    }

                    runAndIgnoreExceptions {
                        stopWorker(
                            workerId = workerId,
                            shutDownMode = shutDownMode
                        )
                    }
                }
            }
        }

        if (waitTillDone) {
            for (runner in allRunners) {
                runAndIgnoreExceptions {
                    runner.waitTillDone()
                }
            }
        }
    }

    override fun close(
        shutDownMode: ShutDownMode,
        waitTillDone: Boolean
    ) {
        val allRunners = mutableListOf<Runner>()

        runAndIgnoreExceptions {
            logger.info("${this.javaClass.simpleName} will try to close using ShutDownMode '${shutDownMode}'")
        }

        synchronized(workers) {
            if (workers.isNotEmpty()) {
                val workerIds = workers.keys.toList()
                for (workerId in workerIds) {
                    if (waitTillDone) {
                        runAndIgnoreExceptions {
                            workers.get(workerId)?.also {
                                allRunners.add(it.runner)
                            }
                        }
                    }

                    runAndIgnoreExceptions {
                        stopWorker(
                            workerId = workerId,
                            shutDownMode = shutDownMode
                        )
                    }
                }
            }

            val wasClosedBefore = isClosed.getAndSet(true)

            if (!wasClosedBefore) {
                runAndIgnoreExceptions {
                    logger.info("${this.javaClass.simpleName} has been closed")
                }
            } else {
                runAndIgnoreExceptions {
                    logger.info("${this.javaClass.simpleName} was already closed")
                }
            }
        }

        if (waitTillDone) {
            for (runner in allRunners) {
                runAndIgnoreExceptions {
                    runner.waitTillDone()
                }
            }
        }
    }



    private fun <T> startWorker(
        workContext: WorkContext<T>,
        worker: Worker<T>
    ): Future<Void>? = workContext.runner.run { shutdownInfo ->

        val workerId = workContext.workerId
        val logId: String = workContext.logId

        runAndIgnoreExceptions {
            logger.info("[${logId}]: Worker just started")
        }

        try {
            var taskNotFoundNTimesInARow = 0L
            while (!shutdownInfo.wasShutdownTriggered() && workContext.canWorkOnNextTask()) {

                try {

                    // PICK TASK

                    var task: T? = null
                    try {
                        task = worker.pickNextTaskToProcess(workerId)
                    } catch (e: InterruptedException) {
                        throw e
                    } catch (e: Exception) {
                        runAndIgnoreExceptions {
                            logger.error("[${logId}]: Failed to pick next task", e)
                        }
                    }


                    // PROCESS TASK

                    workContext.taskInProgress.set(task)

                    if (task == null) {
                        taskNotFoundNTimesInARow += 1


                        if (workContext.endIfNoWorkFound()) {
                            runAndIgnoreExceptions {
                                logger.debug("[${logId}]: Stopping worker as NO available task was found")
                            }
                            break
                        } else {
                            runAndIgnoreExceptions {
                                logger.debug("[${logId}]: NO available task was found")
                            }
                        }
                    } else {
                        taskNotFoundNTimesInARow = 0

                        runAndIgnoreExceptions {
                            val taskString: String? = taskToLoggableString(worker, task, workerId)
                            if (isBlank(taskString)) {
                                logger.info("[${logId}]: Going to process next available task")
                            } else {
                                logger.info("[${logId}]: Going to process next available task '${taskString}'")
                            }
                        }

                        try {
                            if (workContext.hasHeartBeatSupport) {
                                startHeartBeater(
                                    workContext = workContext,
                                    worker = worker,
                                    task = task,
                                    logId = logId
                                )
                            }

                            worker.executeTask(
                                task = task,
                                workerId = workerId
                            )

                            runAndIgnoreExceptions {
                                val taskString: String? = taskToLoggableString(worker, task, workerId)
                                if (isBlank(taskString)) {
                                    logger.info("[${logId}]: Finished processing task")
                                } else {
                                    logger.info("[${logId}]: Finished processing task '${taskString}'")
                                }
                            }
                        } catch (e: InterruptedException) {
                            throw e
                        } catch (e: Exception) {
                            runAndIgnoreExceptions {
                                val taskString: String? = taskToLoggableString(worker, task, workerId)
                                if (isBlank(taskString)) {
                                    logger.warn("[${logId}]: Failed to execute task", e)
                                } else {
                                    logger.warn("[${logId}]: Failed to execute task '${taskString}'", e)
                                }
                            }

                            try {
                                worker.handleExecutionFailure(
                                    task = task,
                                    workerId = workerId,
                                    exception = e
                                )
                            } catch (e: InterruptedException) {
                                throw e
                            } catch (e: Exception) {
                                runAndIgnoreExceptions {
                                    val taskString: String? = taskToLoggableString(worker, task, workerId)
                                    if (isBlank(taskString)) {
                                        logger.error("[${logId}]: Failed to handle failure of task", e)
                                    } else {
                                        logger.error("[${logId}]: Failed to handle failure of task '${taskString}'", e)
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


                    // SLEEP DELAY BEFORE PICKING NEXT TASK

                    // default to 1 minute if fails to get the sleep duration
                    var sleepDuration: Duration = ONE_MINUTE
                    try {
                        if (task != null) {
                            sleepDuration = worker.sleepDurationIfTaskWasProcessed(
                                workerId
                            )
                        } else {
                            sleepDuration = worker.sleepDurationIfNoTaskWasAvailable(
                                taskNotFoundNTimesInARow,
                                workerId
                            )
                        }
                    } catch (e: Exception) {
                        runAndIgnoreExceptions {
                            logger.error("${logId}]: Failed to evaluate sleep duration before picking next task", e)
                        }
                    }
                    // don't use Thread.sleep(..) as it would wait for the whole duration in case of graceful shutdown
                    // (graceful shutdown = no InterruptedException)
                    workContext.sleepIfWillAskForNextTask(sleepDuration)

                } catch (e: InterruptedException) {
                    runAndIgnoreExceptions {
                        val isExpected = shutdownInfo.wasShutdownTriggered()
                        if (isExpected) {
                            logger.info("[${logId}]: Worker thread has been interrupted")
                        } else {
                            logger.error("[${logId}]: Worker thread has been interrupted", e)
                        }
                    }
                    break
                } catch (e: Exception) {
                    runAndIgnoreExceptions {
                        logger.error("${logId}]: Unexpected failure", e)
                    }
                }
            }
        } finally {
            if (workContext.isBeingGracefullyShutDown()) {
                runAndIgnoreExceptions {
                    synchronized(workers) {
                        workers.remove(workerId)
                    }
                }

                runAndIgnoreExceptions {
                    logger.info("[${logId}]: Worker thread has been shut down gracefully")
                }

                runAndIgnoreExceptions {
                    worker.close()
                }

                runAndIgnoreExceptions {
                    workContext.runner.shutdownNow()
                }
            } else {
                runAndIgnoreExceptions {
                    worker.close()
                }

                runAndIgnoreExceptions {
                    logger.info("[${logId}]: Worker thread has been shut down")
                }
            }
        }
    }

    private fun <T> startHeartBeater(
        workContext: WorkContext<T>,
        worker: Worker<T>,
        task: T,
        logId: String
    ) {
        val workerId = workContext.workerId

        val heartBeaterId = randomUUID()
        workContext.lastHeartBeaterId.set(heartBeaterId)

        val heartBeatingWorker = worker as HeartBeatingWorker<T>
        // we should fail and don't start task processing if we're unable to get the heart beat interval
        val heartBeatInterval = heartBeatingWorker.heartBeatInterval(task, workerId)
        val heartBeater: Future<Void> = workContext.runner.run { shutdownInfo ->
            while (
                !shutdownInfo.wasShutdownTriggered() &&
                heartBeaterId.equals(workContext.lastHeartBeaterId.get())
            ) {
                try {
                    Thread.sleep(heartBeatInterval.toMillis())

                    heartBeatingWorker.updateHeartBeat(
                        task = task,
                        workerId = workerId
                    )
                } catch (e: InterruptedException) {
                    runAndIgnoreExceptions {
                        if (heartBeaterId.equals(workContext.lastHeartBeaterId.get())) {
                            logger.error("[${logId}]: Heart beat thread has been interrupted", e)
                        } else {
                            logger.info("[${logId}]: Heart beat thread has finished")
                        }
                    }
                    break
                } catch (e: Exception) {
                    runAndIgnoreExceptions {
                        val taskString: String? = taskToLoggableString(worker, task, workerId)
                        if (isBlank(taskString)) {
                            logger.error("[${logId}]: Failed to update heart beat for task", e)
                        } else {
                            logger.error("[${logId}]: Failed to update heart beat for task '${taskString}'", e)
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
        worker: Worker<T>,
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
}