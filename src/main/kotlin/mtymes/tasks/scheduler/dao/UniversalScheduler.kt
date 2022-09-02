package mtymes.tasks.scheduler.dao

import com.mongodb.client.MongoCollection
import com.mongodb.client.model.FindOneAndUpdateOptions
import com.mongodb.client.model.ReturnDocument
import mtymes.tasks.common.check.ValidityChecks.expectAtLeastOneItem
import mtymes.tasks.common.check.ValidityChecks.expectNonEmptyDocument
import mtymes.tasks.common.domain.WorkerId
import mtymes.tasks.common.exception.ExceptionUtil.runAndIgnoreExceptions
import mtymes.tasks.common.mongo.DocBuilder.Companion.doc
import mtymes.tasks.common.mongo.DocBuilder.Companion.docBuilder
import mtymes.tasks.common.mongo.DocBuilder.Companion.emptyDoc
import mtymes.tasks.common.mongo.DocumentExt.areDefined
import mtymes.tasks.common.mongo.DocumentExt.isDefined
import mtymes.tasks.common.mongo.MongoCollectionExt.findOne
import mtymes.tasks.common.mongo.MongoCollectionExt.insert
import mtymes.tasks.common.time.Clock
import mtymes.tasks.common.time.UTCClock
import mtymes.tasks.scheduler.domain.*
import mtymes.tasks.scheduler.exception.*
import org.bson.Document
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.ZonedDateTime
import java.util.*

// todo: mtymes - don't increment EXECUTION_ATTEMPTS_LEFT on suspension
// todo: mtymes - add execution field: was unRetriableFail
// todo: mtymes - add indexes - should be done by users of this class (e.g.: ttl index, unique executionId index, ...)
class UniversalScheduler(
    val clock: Clock = UTCClock
) {

    companion object {

        val UNIVERSAL_SCHEDULER = UniversalScheduler()

        val logger = LoggerFactory.getLogger(UniversalScheduler::class.java) as Logger

        // todo: mtymes - check the fields are defined on correct level (task/execution)

        // SHARED FIELDS

        const val DATA = "data"
        const val STATUS = "status"
        const val STATUS_UPDATED_AT = "statusUpdatedAt"
        const val UPDATED_AT = "updatedAt"

        // TASK FIELDS

        const val TASK_ID = "_id"
        const val CREATED_AT = "createdAt"
        const val DELETE_AFTER = "deleteAfter"

        const val MAX_EXECUTION_ATTEMPTS_COUNT = "maxAttemptsCount"
        const val EXECUTION_ATTEMPTS_LEFT = "attemptsLeft"

        const val CAN_BE_EXECUTED_AS_OF = "canBeExecutedAsOf"

        const val LAST_EXECUTION_ID = "lastExecutionId"
        const val LAST_EXECUTION_STATUS = "lastExecutionStatus"
        const val LAST_EXECUTION_TIMES_OUT_AFTER = "lastExecutionTimesOutAfter"

        // todo: mtymes - add flag - retainOnlyLastExecution

        // when recording all executions
        const val EXECUTIONS = "executions"

        // when recording only last execution
        // todo: mtymes - add this field for retainOnlyLastExecution
//        const val EXECUTION_COUNT = "executionCount"
        // todo: mtymes - add this field for retainOnlyLastExecution
//        const val LAST_EXECUTION = "lastExecution"

        // EXECUTION FIELDS

        const val EXECUTION_ID = "id"

        // todo: mtymes - differentiate between startedBy: Worker, suspendedBy: Worker, resumedBy: Worker, cancelledBy/finalizedBy: Worker
        const val WORKER_ID = "workerId"
        const val STARTED_AT = "startedAt"
        const val SUSPENDED_AT = "suspendedAt"
        const val LAST_UN_SUSPENDED_AT = "lastUnSuspendedAt"
        const val SUSPENSION_COUNT = "suspensionCount"
        const val LAST_HEARTBEAT_AT = "lastHeartBeatAt"
        const val TIMES_OUT_AFTER = "timesOutAfter"
        const val FINISHED_AT = "finishedAt"

    }


    private sealed interface ToTaskStatus
    private data class ToSingleTaskStatus(
        val status: TaskStatus
    ) : ToTaskStatus

    private data class ToAvailabilityBasedTaskStatus(
        val statusIfAttemptsAvailable: TaskStatus,
        val statusIfNOAttemptsAvailable: TaskStatus
    ) : ToTaskStatus


    // todo: mtymes - fail if task is not submitted
    fun submitTask(
        coll: MongoCollection<Document>,
        data: Document,
        options: SubmitTaskOptions
    ): TaskId {
        expectNonEmptyDocument("data", data)

        val taskId = options.taskIdGenerator.invoke()
        val status = if (options.submitAsPaused) TaskStatus.paused else TaskStatus.available

        val now = clock.now()

        val success = coll.insert(
            doc(
                TASK_ID to taskId,
                CREATED_AT to now,
                DATA to data,
                DELETE_AFTER to now.plus(options.ttl),
                MAX_EXECUTION_ATTEMPTS_COUNT to options.maxAttemptsCount,

                EXECUTION_ATTEMPTS_LEFT to options.maxAttemptsCount,
                CAN_BE_EXECUTED_AS_OF to now.plus(options.delayStartBy),

                STATUS to status,
                STATUS_UPDATED_AT to now,

                UPDATED_AT to now,
            )
        )

        if (success) {
            return taskId
        } else {
            throw IllegalStateException("Failed to submit Task with TaskId '${taskId}' and content ${data}")
        }
    }

    fun fetchNextAvailableExecution(
        coll: MongoCollection<Document>,
        workerId: WorkerId,
        options: FetchNextExecutionOptions,
        additionalConstraints: Document? = null,
        sortOrder: Document? = null
    ): FetchedExecutionSummary? {
        val usedSortOrder = sortOrder ?: doc(CAN_BE_EXECUTED_AS_OF to 1)

        if (options.fetchSuspendedTasksAsWell) {
            val now = clock.now()

            val possibleTasksToFetch = coll.find(
                docBuilder()
                    .putAllIf(additionalConstraints.areDefined()) {
                        additionalConstraints!!
                    }
                    .putAll(
                        STATUS to doc("\$in", listOf(TaskStatus.available, TaskStatus.suspended)),
                        CAN_BE_EXECUTED_AS_OF to doc("\$lte", now),
                        EXECUTION_ATTEMPTS_LEFT to doc("\$gte", 1)
                    )
                    .build()
            ).sort(
                usedSortOrder
            ).projection(
                doc(
                    TASK_ID to 1,
                    STATUS to 1,
                    LAST_EXECUTION_ID to 1
                )
            ).limit(5)

            for (possibleTaskToFetch in possibleTasksToFetch) {
                val taskId = TaskId(possibleTaskToFetch.getString(TASK_ID))
                val taskStatus = TaskStatus.valueOf(possibleTaskToFetch.getString(STATUS))

                if (taskStatus == TaskStatus.available) {
                    val execution = startNewExecution(
                        coll = coll,
                        workerId = workerId,
                        taskId = Optional.of(taskId),
                        keepAliveFor = options.keepAliveFor,
                        additionalConstraints = additionalConstraints,
                        sortOrder = usedSortOrder,
                        newTTL = options.newTTL
                    )

                    if (execution != null) {
                        return execution
                    }
                } else if (taskStatus == TaskStatus.suspended) {
                    val lastExecutionId = ExecutionId(possibleTaskToFetch.getString(LAST_EXECUTION_ID))

                    val execution = resumeSuspendedExecution(
                        coll = coll,
                        workerId = workerId,
                        taskId = taskId,
                        lastExpectedExecutionId = lastExecutionId,
                        keepAliveFor = options.keepAliveFor,
                        additionalConstraints = additionalConstraints,
                        newTTL = options.newTTL
                    )

                    if (execution != null) {
                        return execution
                    }
                }
            }

            // todo: mtymes - if got some tasks, but all were already fetched (concurrently by other thread), then maybe try again

            return null
        } else {
            val execution = startNewExecution(
                coll = coll,
                workerId = workerId,
                taskId = Optional.empty(),
                keepAliveFor = options.keepAliveFor,
                additionalConstraints = additionalConstraints,
                sortOrder = usedSortOrder,
                newTTL = options.newTTL
            )

            return execution
        }
    }

    fun markAsSucceeded(
        coll: MongoCollection<Document>,
        executionId: ExecutionId,
        options: MarkAsSucceededOptions,
        additionalTaskData: Document? = null,
        additionalExecutionData: Document? = null
    ): ExecutionSummary? {
        val now = clock.now()

        return updateExecution(
            coll = coll,
            executionId = executionId,
            fromTaskStatus = TaskStatus.inProgress,
            fromExecutionStatus = ExecutionStatus.running,
            toTaskStatus = TaskStatus.succeeded,
            toExecutionStatus = ExecutionStatus.succeeded,
            now = now,
            customTaskUpdates = options.newTTL?.let { newTTL ->
                doc(DELETE_AFTER to now.plus(newTTL))
            },
            customExecutionUpdates = doc(
                FINISHED_AT to now
            ),
            additionalTaskData = additionalTaskData,
            additionalExecutionData = additionalExecutionData
        )
    }

    fun markAsFailedButCanRetry(
        coll: MongoCollection<Document>,
        executionId: ExecutionId,
        options: MarkAsFailedButCanRetryOptions,
        additionalTaskData: Document? = null,
        additionalExecutionData: Document? = null
    ): ExecutionSummary? {
        val now = clock.now()

        return updateExecution(
            coll = coll,
            executionId = executionId,
            fromTaskStatus = TaskStatus.inProgress,
            fromExecutionStatus = ExecutionStatus.running,
            toTaskStatus = ToAvailabilityBasedTaskStatus(
                statusIfAttemptsAvailable = TaskStatus.available,
                statusIfNOAttemptsAvailable = TaskStatus.failed
            ),
            toExecutionStatus = ExecutionStatus.failed,
            now = now,
            customTaskUpdates = docBuilder()
                .putIf(options.retryDelay != null) {
                    CAN_BE_EXECUTED_AS_OF to doc(
                        "\$cond" to listOf(
                            doc("\$gt" to listOf("\$" + EXECUTION_ATTEMPTS_LEFT, 0)),
                            now.plus(options.retryDelay!!),
                            "\$" + CAN_BE_EXECUTED_AS_OF
                        )
                    )
                }
                .putIf(options.newTTL != null) {
                    DELETE_AFTER to now.plus(options.newTTL!!)
                }
                .build(),
            customExecutionUpdates = doc(
                FINISHED_AT to now
            ),
            additionalTaskData = additionalTaskData,
            additionalExecutionData = additionalExecutionData
        )
    }

    fun markAsFailedButCanNOTRetry(
        coll: MongoCollection<Document>,
        executionId: ExecutionId,
        options: MarkAsFailedButCanNOTRetryOptions,
        additionalTaskData: Document? = null,
        additionalExecutionData: Document? = null
    ): ExecutionSummary? {
        val now = clock.now()

        return updateExecution(
            coll = coll,
            executionId = executionId,
            fromTaskStatus = TaskStatus.inProgress,
            fromExecutionStatus = ExecutionStatus.running,
            toTaskStatus = ToSingleTaskStatus(TaskStatus.failed),
            toExecutionStatus = ExecutionStatus.failed,
            now = now,
            customTaskUpdates = options.newTTL?.let { newTTL ->
                doc(DELETE_AFTER to now.plus(newTTL))
            },
            customExecutionUpdates = doc(
                FINISHED_AT to now
            ),
            additionalTaskData = additionalTaskData,
            additionalExecutionData = additionalExecutionData
        )
    }

    fun markTaskAsCancelled(
        coll: MongoCollection<Document>,
        taskId: TaskId,
        options: MarkTaskAsCancelledOptions,
        additionalTaskData: Document? = null
    ): Task? {
        val now = clock.now()

        val fromTaskStatuses = listOf(TaskStatus.available, TaskStatus.paused)
        val toTaskStatus = TaskStatus.cancelled

        return updateTask(
            coll = coll,
            taskId = taskId,
            fromTaskStatuses = fromTaskStatuses,
            toTaskStatus = toTaskStatus,
            now = now,
            customTaskUpdates = options.newTTL?.let { newTTL ->
                doc(DELETE_AFTER to now.plus(newTTL))
            },
            additionalTaskData = additionalTaskData
        )
    }

    // todo: mtymes - add sample for this one
    fun markTasksAsCancelled(
        coll: MongoCollection<Document>,
        options: MarkTasksAsCancelledOptions,
        customConstraints: Document,
        additionalTaskData: Document? = null
    ): Long {
        val now = clock.now()

        val fromTaskStatuses = listOf(TaskStatus.available, TaskStatus.paused)
        val toTaskStatus = TaskStatus.cancelled

        return updateTasks(
            coll = coll,
            customConstraints = customConstraints,
            fromTaskStatuses = fromTaskStatuses,
            toTaskStatus = toTaskStatus,
            now = now,
            customTaskUpdates = options.newTTL?.let { newTTL ->
                doc(DELETE_AFTER to now.plus(newTTL))
            },
            additionalTaskData = additionalTaskData
        )
    }

    fun markAsCancelled(
        coll: MongoCollection<Document>,
        executionId: ExecutionId,
        options: MarkAsCancelledOptions,
        additionalTaskData: Document? = null,
        additionalExecutionData: Document? = null
    ): ExecutionSummary? {
        val now = clock.now()

        return updateExecution(
            coll = coll,
            executionId = executionId,
            fromTaskStatus = TaskStatus.inProgress,
            fromExecutionStatus = ExecutionStatus.running,
            toTaskStatus = TaskStatus.cancelled,
            toExecutionStatus = ExecutionStatus.cancelled,
            now = now,
            customTaskUpdates = options.newTTL?.let { newTTL ->
                doc(DELETE_AFTER to now.plus(newTTL))
            },
            customExecutionUpdates = doc(
                FINISHED_AT to now
            ),
            additionalTaskData = additionalTaskData,
            additionalExecutionData = additionalExecutionData
        )
    }

    // todo: mtymes - add sample for this one
    fun markTaskAsPaused(
        coll: MongoCollection<Document>,
        taskId: TaskId,
        options: MarkTaskAsPausedOptions,
        additionalTaskData: Document? = null
    ): Task? {
        val now = clock.now()

        val fromTaskStatuses = listOf(TaskStatus.available)
        val toTaskStatus = TaskStatus.paused

        return updateTask(
            coll = coll,
            taskId = taskId,
            fromTaskStatuses = fromTaskStatuses,
            toTaskStatus = toTaskStatus,
            now = now,
            customTaskUpdates = options.newTTL?.let { newTTL ->
                doc(DELETE_AFTER to now.plus(newTTL))
            },
            additionalTaskData = additionalTaskData
        )
    }

    // todo: mtymes - add sample for this one
    fun markTasksAsPaused(
        coll: MongoCollection<Document>,
        options: MarkTasksAsPausedOptions,
        customConstraints: Document,
        additionalTaskData: Document? = null

    ): Long {
        val now = clock.now()

        val fromTaskStatuses = listOf(TaskStatus.available)
        val toTaskStatus = TaskStatus.paused

        return updateTasks(
            coll = coll,
            customConstraints = customConstraints,
            fromTaskStatuses = fromTaskStatuses,
            toTaskStatus = toTaskStatus,
            now = now,
            customTaskUpdates = options.newTTL?.let { newTTL ->
                doc(DELETE_AFTER to now.plus(newTTL))
            },
            additionalTaskData = additionalTaskData
        )
    }

    // todo: mtymes - add sample for this one
    fun markTaskAsUnPaused(
        coll: MongoCollection<Document>,
        taskId: TaskId,
        options: MarkTaskAsUnPausedOptions,
        additionalTaskData: Document? = null
    ): Task? {
        val now = clock.now()

        val fromTaskStatuses = listOf(TaskStatus.paused)
        val toTaskStatus = TaskStatus.available

        return updateTask(
            coll = coll,
            taskId = taskId,
            fromTaskStatuses = fromTaskStatuses,
            toTaskStatus = toTaskStatus,
            now = now,
            customTaskUpdates = options.newTTL?.let { newTTL ->
                doc(DELETE_AFTER to now.plus(newTTL))
            },
            additionalTaskData = additionalTaskData
        )
    }

    // todo: mtymes - add sample for this one
    fun markTasksAsUnPaused(
        coll: MongoCollection<Document>,
        options: MarkTasksAsUnPausedOptions,
        customConstraints: Document,
        additionalTaskData: Document? = null

    ): Long {
        val now = clock.now()

        val fromTaskStatuses = listOf(TaskStatus.paused)
        val toTaskStatus = TaskStatus.available

        return updateTasks(
            coll = coll,
            customConstraints = customConstraints,
            fromTaskStatuses = fromTaskStatuses,
            toTaskStatus = toTaskStatus,
            now = now,
            customTaskUpdates = options.newTTL?.let { newTTL ->
                doc(DELETE_AFTER to now.plus(newTTL))
            },
            additionalTaskData = additionalTaskData
        )
    }

    fun markAsSuspended(
        coll: MongoCollection<Document>,
        executionId: ExecutionId,
        options: MarkAsSuspendedOptions,
        additionalTaskData: Document? = null,
        additionalExecutionData: Document? = null
    ): ExecutionSummary? {
        val now = clock.now()

        return updateExecution(
            coll = coll,
            executionId = executionId,
            fromTaskStatus = TaskStatus.inProgress,
            fromExecutionStatus = ExecutionStatus.running,
            toTaskStatus = TaskStatus.suspended,
            toExecutionStatus = ExecutionStatus.suspended,
            now = now,
            customTaskUpdates = docBuilder()
                .putAll(
                    CAN_BE_EXECUTED_AS_OF to now.plus(options.suspendFor),

                    // $inc - section
                    // todo: mtymes - handle differently
                    EXECUTION_ATTEMPTS_LEFT to doc("\$sum" to listOf("\$" + EXECUTION_ATTEMPTS_LEFT, 1))
                )
                .putIf(options.newTTL != null) {
                    DELETE_AFTER to now.plus(options.newTTL!!)
                }
                .build(),
            customExecutionUpdates = doc(
                SUSPENDED_AT to now,

                // $inc - section
                // todo: mtymes - handle differently
                SUSPENSION_COUNT to doc("\$sum" to listOf("\$\$ex." + SUSPENSION_COUNT, 1))
            ),
            additionalTaskData = additionalTaskData,
            additionalExecutionData = additionalExecutionData
        )
    }

    // todo: mtymes - return number of TimedOut Executions
    // todo: mtymes - allow to make this code a bit more dynamic - so the client could evaluate for example data to update based on task/execution data
    // todo: mtymes - maybe add custom query criteria
    fun markDeadExecutionsAsTimedOut(
        coll: MongoCollection<Document>,
        options: MarkDeadExecutionsAsTimedOutOptions,
        additionalTaskData: Document? = null,
        additionalExecutionData: Document? = null
    ) {
        val deadTasks: List<Document> = coll.find(
            doc(
                STATUS to doc("\$in", listOf(TaskStatus.inProgress, TaskStatus.suspended)),
                LAST_EXECUTION_STATUS to doc("\$in", listOf(ExecutionStatus.running, ExecutionStatus.suspended)),
                LAST_EXECUTION_TIMES_OUT_AFTER to (doc("\$lt" to clock.now())),
            )
        ).toList()

        for (deadTask: Document in deadTasks) {
            try {
                val currentTaskStatus = TaskStatus.valueOf(deadTask.getString(STATUS))
                val currentExecutionStatus = ExecutionStatus.valueOf(deadTask.getString(LAST_EXECUTION_STATUS))
                val lastExecutionId = ExecutionId(deadTask.getString(LAST_EXECUTION_ID))
                val executionAttemptsLeft = deadTask.getInteger(EXECUTION_ATTEMPTS_LEFT)

                val toTaskStatus = if (currentTaskStatus == TaskStatus.suspended) {
                    if (executionAttemptsLeft > 1) TaskStatus.available else TaskStatus.timedOut
                } else {
                    if (executionAttemptsLeft > 0) TaskStatus.available else TaskStatus.timedOut
                }

                val now = clock.now()

                updateExecution(
                    coll = coll,
                    executionId = lastExecutionId,
                    fromTaskStatus = currentTaskStatus,
                    fromExecutionStatus = currentExecutionStatus,
                    toTaskStatus = toTaskStatus,
                    toExecutionStatus = ExecutionStatus.timedOut,
                    now = now,
                    customTaskUpdates = docBuilder()
                        .putIf(options.retryDelay != null) {
                            CAN_BE_EXECUTED_AS_OF to now.plus(options.retryDelay!!)
                        }
                        .putIf(options.newTTL != null) {
                            DELETE_AFTER to now.plus(options.newTTL!!)
                        }
                        .putIf(currentTaskStatus == TaskStatus.suspended) {
                            EXECUTION_ATTEMPTS_LEFT to executionAttemptsLeft - 1
                        }
                        .build(),
                    customExecutionUpdates = doc(
                        FINISHED_AT to now
                    ),
                    additionalTaskData = additionalTaskData,
                    additionalExecutionData = additionalExecutionData
                )
            } catch (e: Exception) {
                runAndIgnoreExceptions {
                    val executionId = ExecutionId(deadTask.getString(LAST_EXECUTION_ID))
                    logger.error("Failed to mark Execution '${executionId}' as '${ExecutionStatus.suspended}'", e)
                }
            }
        }
    }

    fun registerHeartBeat(
        coll: MongoCollection<Document>,
        executionId: ExecutionId,
        options: RegisterHeartBeatOptions,
        additionalTaskData: Document? = null,
        additionalExecutionData: Document? = null
    ): Boolean {
        val now = clock.now()
        val keepAliveUntil = now.plus(options.keepAliveFor)

        val result = coll.updateOne(
            queryForExecution(
                executionId,
                TaskStatus.inProgress,
                ExecutionStatus.running
            ),
            doc(
                "\$set" to docBuilder()
                    .putAll(
                        LAST_EXECUTION_TIMES_OUT_AFTER to keepAliveUntil,
                        EXECUTIONS + ".\$." + LAST_HEARTBEAT_AT to now,
                        EXECUTIONS + ".\$." + TIMES_OUT_AFTER to keepAliveUntil,
                    )
                    .putAllIf(additionalTaskData.isDefined()) {
                        additionalTaskData!!.mapKeys { entry ->
                            DATA + "." + entry.key
                        }
                    }
                    .putAllIf(additionalExecutionData.isDefined()) {
                        additionalExecutionData!!.mapKeys { entry ->
                            EXECUTIONS + ".\$." + DATA + "." + entry.key
                        }
                    }
                    .putAllIf(additionalTaskData.isDefined() || additionalExecutionData.isDefined()) {
                        doc(
                            EXECUTIONS + ".\$." + UPDATED_AT to now,
                            UPDATED_AT to now
                        )
                    }
                    .putIf(options.newTTL != null) {
                        DELETE_AFTER to now.plus(options.newTTL!!)
                    }
                    .build()
            )
        )

        return result.modifiedCount > 0
    }

    // todo: mtymes - change return type from Task? -> Task and declare thrown Exceptions on the method
    // todo: mtymes - maybe add custom query criteria
    fun updateTaskData(
        coll: MongoCollection<Document>,
        taskId: TaskId,
        options: UpdateTaskDataOptions,
        additionalTaskData: Document
    ): Task? {
        expectNonEmptyDocument("additionalTaskData", additionalTaskData)

        val now = clock.now()

        val result = coll.findOneAndUpdate(
            doc(
                TASK_ID to taskId
            ),
            doc(
                "\$set" to docBuilder()
                    .putAll(
                        UPDATED_AT to now
                    )
                    .putAll(
                        additionalTaskData.mapKeys { entry ->
                            DATA + "." + entry.key
                        }
                    )
                    .putIf(options.newTTL != null) {
                        DELETE_AFTER to now.plus(options.newTTL!!)
                    }
                    .build()
            ),
            FindOneAndUpdateOptions()
                .returnDocument(ReturnDocument.AFTER)
        )

        return result?.toTask()
    }

    // todo: mtymes - change return type from Task? -> Task and declare thrown Exceptions on the method
    // todo: mtymes - maybe add custom query criteria
    fun updateExecutionData(
        coll: MongoCollection<Document>,
        executionId: ExecutionId,
        options: UpdateExecutionDataOptions,
        additionalExecutionData: Document,
        additionalTaskData: Document? = null
    ): ExecutionSummary? {
        expectNonEmptyDocument("additionalExecutionData", additionalExecutionData)

        val now = clock.now()

        val result = coll.findOneAndUpdate(
            if (options.mustBeLastExecution) {
                doc(
                    LAST_EXECUTION_ID to executionId,
                    EXECUTIONS to doc("\$elemMatch" to doc(EXECUTION_ID to executionId))
                )
            } else {
                doc(
                    EXECUTIONS to doc("\$elemMatch" to doc(EXECUTION_ID to executionId))
                )
            },
            doc(
                "\$set" to docBuilder()
                    .putAllIf(additionalTaskData.isDefined()) {
                        additionalTaskData!!.mapKeys { entry ->
                            DATA + "." + entry.key
                        }
                    }
                    .putAll(
                        additionalExecutionData.mapKeys { entry ->
                            EXECUTIONS + ".\$." + DATA + "." + entry.key
                        }
                    )
                    .putAll(
                        EXECUTIONS + ".\$." + UPDATED_AT to now,
                        UPDATED_AT to now
                    )
                    .putIf(options.newTTL != null) {
                        DELETE_AFTER to now.plus(options.newTTL!!)
                    }
                    .build(),
            ),
            FindOneAndUpdateOptions()
                .returnDocument(ReturnDocument.AFTER)
        )

        return result?.toExecutionSummary(
            executionId = executionId
        )
    }

    fun getTask(
        coll: MongoCollection<Document>,
        taskId: TaskId
    ): Task? {
        return coll.findOne(
            doc(
                TASK_ID to taskId
            )
        )?.toTask()
    }

    fun getTasks(
        coll: MongoCollection<Document>,
        customConstraints: Document
    ): Iterable<Task> {
        return coll.find(
            customConstraints
        ).map {
            it.toTask()
        }
    }

    fun getExecution(
        coll: MongoCollection<Document>,
        executionId: ExecutionId
    ): ExecutionSummary? {
        return coll.findOne(
            doc(
                EXECUTIONS + "." + EXECUTION_ID to executionId
            )
        )?.toExecutionSummary(
            executionId = executionId
        )
    }


    private fun startNewExecution(
        coll: MongoCollection<Document>,
        workerId: WorkerId,
        taskId: Optional<TaskId>,
        keepAliveFor: Duration,
        additionalConstraints: Document?,
        sortOrder: Document,
        newTTL: Duration?
    ): FetchedExecutionSummary? {
        val now = clock.now()
        val keepAliveUntil = now.plus(keepAliveFor)

        val executionId = ExecutionId(UUID.randomUUID())

        val modifiedTask = coll.findOneAndUpdate(
            docBuilder()
                .putAllIf(additionalConstraints.areDefined()) {
                    additionalConstraints!!
                }
                .putAll(
                    TASK_ID to taskId,
                    STATUS to TaskStatus.available,
                    CAN_BE_EXECUTED_AS_OF to doc("\$lte", now),
                    EXECUTION_ATTEMPTS_LEFT to doc("\$gte", 1)
                )
                .build(),
            doc(
                "\$addToSet" to doc(
                    EXECUTIONS to doc(
                        EXECUTION_ID to executionId,
                        STARTED_AT to now,
                        WORKER_ID to workerId,
                        STATUS to ExecutionStatus.running,
                        STATUS_UPDATED_AT to now,
                        DATA to emptyDoc(),
                        TIMES_OUT_AFTER to keepAliveUntil,
                        UPDATED_AT to now,
                    )
                ),
                "\$set" to docBuilder()
                    .putAll(
                        STATUS to TaskStatus.inProgress,
                        STATUS_UPDATED_AT to now,
                        LAST_EXECUTION_ID to executionId,
                        LAST_EXECUTION_STATUS to ExecutionStatus.running,
                        LAST_EXECUTION_TIMES_OUT_AFTER to keepAliveUntil,
                        UPDATED_AT to now,
                    )
                    .putIf(newTTL != null) {
                        DELETE_AFTER to now.plus(newTTL!!)
                    }
                    .build(),
//                "\$unset" to doc(
//                    CAN_BE_EXECUTED_AS_OF to 1
//                ),
                "\$inc" to doc(
                    EXECUTION_ATTEMPTS_LEFT to -1
                )
            ),
            FindOneAndUpdateOptions()
                .returnDocument(ReturnDocument.AFTER)
                .sort(sortOrder)
        )

        return modifiedTask?.toFetchedExecutionSummary(
            expectedLastExecutionId = executionId,
            wasSuspended = false
        )
    }

    private fun resumeSuspendedExecution(
        coll: MongoCollection<Document>,
        workerId: WorkerId,
        taskId: TaskId,
        lastExpectedExecutionId: ExecutionId,
        keepAliveFor: Duration,
        additionalConstraints: Document?,
        newTTL: Duration?
    ): FetchedExecutionSummary? {
        val now = clock.now()
        val keepAliveUntil = now.plus(keepAliveFor)

        val modifiedTask = coll.findOneAndUpdate(
            docBuilder()
                .putAllIf(additionalConstraints.areDefined()) {
                    additionalConstraints!!
                }
                .putAll(
                    TASK_ID to taskId,
                    STATUS to TaskStatus.suspended,
                    LAST_EXECUTION_ID to lastExpectedExecutionId,
                    LAST_EXECUTION_STATUS to ExecutionStatus.suspended,
                    EXECUTIONS to doc(
                        "\$elemMatch" to doc(
                            EXECUTION_ID to lastExpectedExecutionId,
                            STATUS to ExecutionStatus.suspended,
                        )
                    ),
                    CAN_BE_EXECUTED_AS_OF to doc("\$lte", now),
                    EXECUTION_ATTEMPTS_LEFT to doc("\$gte", 1)
                )
                .build(),
            doc(
                "\$set" to docBuilder()
                    .putAll(
                        STATUS to TaskStatus.inProgress,
                        STATUS_UPDATED_AT to now,
                        LAST_EXECUTION_STATUS to ExecutionStatus.running,
                        LAST_EXECUTION_TIMES_OUT_AFTER to keepAliveUntil,
                        EXECUTIONS + ".\$." + STATUS to ExecutionStatus.running,
                        EXECUTIONS + ".\$." + STATUS_UPDATED_AT to now,
                        EXECUTIONS + ".\$." + LAST_UN_SUSPENDED_AT to now,
                        EXECUTIONS + ".\$." + WORKER_ID to workerId,
                        EXECUTIONS + ".\$." + TIMES_OUT_AFTER to keepAliveUntil,
                        EXECUTIONS + ".\$." + UPDATED_AT to now,
                        UPDATED_AT to now

                    )
                    .putIf(newTTL != null) {
                        DELETE_AFTER to now.plus(newTTL!!)
                    }
                    .build(),
//                "\$unset" to doc(
//                    CAN_BE_EXECUTED_AS_OF to 1
//                ),
                "\$inc" to doc(
                    EXECUTION_ATTEMPTS_LEFT to -1
                )
            ),
            FindOneAndUpdateOptions()
                .returnDocument(ReturnDocument.AFTER)
        )

        return modifiedTask?.toFetchedExecutionSummary(
            expectedLastExecutionId = lastExpectedExecutionId,
            wasSuspended = true
        )
    }

    // todo: mtymes - change return type from Task? -> Task and declare thrown Exceptions on the method
    private fun updateTask(
        coll: MongoCollection<Document>,
        taskId: TaskId,
        fromTaskStatuses: List<TaskStatus>,
        toTaskStatus: TaskStatus,
        now: ZonedDateTime,
        customTaskUpdates: Document? = null,
        additionalTaskData: Document?
    ): Task? {
        expectAtLeastOneItem("fromTaskStatuses", fromTaskStatuses)

        val modifiedTask = coll.findOneAndUpdate(
            doc(
                TASK_ID to taskId,
                STATUS to if (fromTaskStatuses.size == 1)
                    fromTaskStatuses[0]
                else
                    doc("\$in" to fromTaskStatuses)
            ),
            doc(
                "\$set" to docBuilder()
                    .putAll(
                        STATUS to toTaskStatus,
                        STATUS_UPDATED_AT to now,
                        UPDATED_AT to now
                    )
                    .putAllIf(additionalTaskData.isDefined()) {
                        additionalTaskData!!.mapKeys { entry ->
                            DATA + "." + entry.key
                        }
                    }
                    .putAllIf(customTaskUpdates.isDefined()) {
                        customTaskUpdates!!
                    }
                    .build()
            ),
            FindOneAndUpdateOptions()
                .returnDocument(ReturnDocument.AFTER)
        )

        if (modifiedTask != null) {
            return modifiedTask.toTask()
        } else {
            val task: Document? = coll.findOne(
                doc(TASK_ID to taskId)
            )

            if (task == null) {
                throw ExecutionNotFoundException(
                    "Task '${taskId}' NOT FOUND"
                )
            }

            val currentTaskStatus = TaskStatus.valueOf(task.getString(STATUS))
            if (!fromTaskStatuses.contains(currentTaskStatus)) {
                if (currentTaskStatus == toTaskStatus) {
                    throw TaskStatusAlreadyAppliedException(
                        "Task '${taskId}' is already in status '${toTaskStatus}'"
                    )
                } else {
                    throw UnexpectedStatusException(
                        if (fromTaskStatuses.size == 1) {
                            "Failed to mark Task '${taskId}' as '${toTaskStatus}' as expected '${fromTaskStatuses[0]}' but got '${currentTaskStatus}' Task status instead"
                        } else {
                            "Failed to mark Task '${taskId}' as '${toTaskStatus}' as expected one of these statuses ${
                                fromTaskStatuses.joinToString(
                                    ", ",
                                    "'",
                                    "'"
                                )
                            } but got '${currentTaskStatus}' Task status instead"
                        }
                    )
                }
            }

            throw UnknownFailureReasonException(
                "Not sure why Task '${taskId}' was not marked as ${toTaskStatus}"
            )
        }
    }

    private fun updateTasks(
        coll: MongoCollection<Document>,
        customConstraints: Document,
        fromTaskStatuses: List<TaskStatus>,
        toTaskStatus: TaskStatus,
        now: ZonedDateTime,
        customTaskUpdates: Document? = null,
        additionalTaskData: Document?
    ): Long {
        expectAtLeastOneItem("fromTaskStatuses", fromTaskStatuses)

        val result = coll.updateMany(
            docBuilder()
                .putAll(customConstraints)
                .putAll(
                    STATUS to if (fromTaskStatuses.size == 1)
                        fromTaskStatuses.get(0)
                    else
                        doc("\$in" to fromTaskStatuses)
                )
                .build(),
            doc("\$set" to docBuilder()
                .putAll(
                    STATUS to toTaskStatus,
                    STATUS_UPDATED_AT to now,
                    UPDATED_AT to now
                )
                .putAllIf(additionalTaskData.isDefined()) {
                    additionalTaskData!!.mapKeys { entry ->
                        DATA + "." + entry.key
                    }
                }
                .putAllIf(customTaskUpdates.isDefined()) {
                    customTaskUpdates!!
                }
                .build()
            )
        )

        return result.modifiedCount
    }

    // todo: mtymes - change return type from Task? -> Task and declare thrown Exceptions on the method
    private fun updateExecution(
        coll: MongoCollection<Document>,
        executionId: ExecutionId,
        fromTaskStatus: TaskStatus,
        fromExecutionStatus: ExecutionStatus,
        toTaskStatus: ToTaskStatus,
        toExecutionStatus: ExecutionStatus,
        now: ZonedDateTime,
        customTaskUpdates: Document? = null,
        customExecutionUpdates: Document? = null,
        additionalTaskData: Document? = null,
        additionalExecutionData: Document? = null
    ): ExecutionSummary? {
        val query = queryForExecution(
            executionId,
            fromTaskStatus,
            fromExecutionStatus
        )
        val update = doc(
            "\$set" to docBuilder()
                .putAll(
                    when (toTaskStatus) {
                        is ToSingleTaskStatus ->
                            STATUS to toTaskStatus.status

                        is ToAvailabilityBasedTaskStatus ->
                            STATUS to doc(
                                "\$cond" to listOf(
                                    doc("\$gt" to listOf("\$" + EXECUTION_ATTEMPTS_LEFT, 0)),
                                    toTaskStatus.statusIfAttemptsAvailable,
                                    toTaskStatus.statusIfNOAttemptsAvailable
                                )
                            )

                    },
                    STATUS_UPDATED_AT to now,
                    LAST_EXECUTION_STATUS to toExecutionStatus,
                    EXECUTIONS to doc(
                        // todo: mtymes - check the performance of this
                        "\$map" to doc(
                            "input" to "\$" + EXECUTIONS,
                            "as" to "ex",
                            "in" to doc(
                                "\$cond" to listOf(
                                    doc("\$eq" to listOf("\$\$ex." + EXECUTION_ID, executionId)),
                                    doc(
                                        "\$mergeObjects" to listOf(
                                            "\$\$ex", docBuilder()
                                                .putAll(
                                                    STATUS to toExecutionStatus,
                                                    STATUS_UPDATED_AT to now,
                                                    UPDATED_AT to now
                                                )
                                                .putIf(additionalExecutionData.isDefined()) {
                                                    DATA to doc(
                                                        "\$mergeObjects" to listOf(
                                                            "\$\$ex.data",
                                                            additionalExecutionData
                                                        )
                                                    )
                                                }
                                                .putAllIf(customExecutionUpdates.areDefined()) {
                                                    customExecutionUpdates!!
                                                }
                                                .build()
                                        )
                                    ),
                                    "\$\$ex"
                                )
                            )
                        )
                    ),
                    UPDATED_AT to now
                )
                .putAllIf(additionalTaskData.isDefined()) {
                    additionalTaskData!!.mapKeys { entry ->
                        DATA + "." + entry.key
                    }
                }
                .putAllIf(customTaskUpdates.isDefined()) {
                    customTaskUpdates!!
                }
                .build()
        )
        val modifiedTask = coll.findOneAndUpdate(
            query,
            listOf(update),
            FindOneAndUpdateOptions()
                .returnDocument(ReturnDocument.AFTER)
        )

        if (modifiedTask != null) {
            return modifiedTask.toExecutionSummary(
                executionId = executionId
            )
        } else {
            val task: Document? = coll.findOne(
                doc(EXECUTIONS + "." + EXECUTION_ID to executionId)
            )

            if (task == null) {
                throw ExecutionNotFoundException(
                    "Execution '${executionId}' NOT FOUND"
                )
            }

            val taskId = TaskId(task.getString(TASK_ID))
            val lastExecutionId = ExecutionId(task.getString(LAST_EXECUTION_ID))
            if (executionId != lastExecutionId) {
                throw NotLastExecutionException(
                    "Execution '${executionId}' for Task '${taskId}' is NOT LAST execution (Execution '${lastExecutionId}' is)"
                )
            }

            val executions = task.getList(EXECUTIONS, Document::class.java)
            val execution = executions.last { executionId == ExecutionId(it.getString(EXECUTION_ID)) }
            val currentExecutionStatus = ExecutionStatus.valueOf(execution.getString(STATUS))
            val currentTaskStatus = TaskStatus.valueOf(task.getString(STATUS))

            val executionAttemptsLeft = task.getInteger(EXECUTION_ATTEMPTS_LEFT)
            val expectedToTaskStatus = when (toTaskStatus) {
                is ToSingleTaskStatus -> toTaskStatus.status
                is ToAvailabilityBasedTaskStatus ->
                    if (executionAttemptsLeft > 0) toTaskStatus.statusIfAttemptsAvailable
                    else toTaskStatus.statusIfNOAttemptsAvailable
            }

            if (currentExecutionStatus != fromExecutionStatus || currentTaskStatus != fromTaskStatus) {
                if (currentExecutionStatus == toExecutionStatus && currentTaskStatus == expectedToTaskStatus) {
                    // already applied
                    throw TaskAndExecutionStatusAlreadyAppliedException(
                        "Task '${taskId}' and Execution '${executionId}' are already in Task status '${expectedToTaskStatus}' and Execution status '${toExecutionStatus}'"
                    )
                } else {
                    throw UnexpectedStatusException(
                        "Failed to mark Task '${taskId}' as '${expectedToTaskStatus}' and Execution '${executionId}' as '${toExecutionStatus}'" +
                                " as expected '${fromTaskStatus}' Task and '${fromExecutionStatus}' Execution but got '${currentTaskStatus}' Task and '${currentExecutionStatus}' Execution instead"
                    )
                }
            }

            throw UnknownFailureReasonException(
                "Not sure why Execution '${executionId}' was not marked as ${toExecutionStatus}"
            )
        }
    }

    private fun updateExecution(
        coll: MongoCollection<Document>,
        executionId: ExecutionId,
        fromTaskStatus: TaskStatus,
        fromExecutionStatus: ExecutionStatus,
        toTaskStatus: TaskStatus,
        toExecutionStatus: ExecutionStatus,
        now: ZonedDateTime,
        customTaskUpdates: Document? = null,
        customExecutionUpdates: Document,
        additionalTaskData: Document?,
        additionalExecutionData: Document?
    ): ExecutionSummary? {
        return updateExecution(
            coll = coll,
            executionId = executionId,
            fromTaskStatus = fromTaskStatus,
            fromExecutionStatus = fromExecutionStatus,
            toTaskStatus = ToSingleTaskStatus(toTaskStatus),
            toExecutionStatus = toExecutionStatus,
            now = now,
            customTaskUpdates = customTaskUpdates,
            customExecutionUpdates = customExecutionUpdates,
            additionalTaskData = additionalTaskData,
            additionalExecutionData = additionalExecutionData
        )
    }

    private fun queryForExecution(
        executionId: ExecutionId,
        taskStatus: TaskStatus,
        executionStatus: ExecutionStatus
    ) = doc(
        STATUS to taskStatus,
        LAST_EXECUTION_ID to executionId,
        EXECUTIONS to doc(
            "\$elemMatch" to doc(
                EXECUTION_ID to executionId,
                STATUS to executionStatus,
            )
        )
    )

    private fun Document.toFetchedExecutionSummary(
        expectedLastExecutionId: ExecutionId,
        wasSuspended: Boolean
    ): FetchedExecutionSummary {
        val lastExecutionId = ExecutionId(this.getString(LAST_EXECUTION_ID))

        if (expectedLastExecutionId != lastExecutionId) {
            throw NotLastExecutionException(
                "No idea how this happened but our started Execution '${expectedLastExecutionId}' was superseded by Execution '${lastExecutionId}'"
            )
        }

        val executionDoc = this
            .getList(EXECUTIONS, Document::class.java)
            .first { ExecutionId(it.getString(EXECUTION_ID)) == lastExecutionId }

        return FetchedExecutionSummary(
            fetchedExecution = executionDoc.toExecution(),
            wasAwokenFromSuspension = wasSuspended,
            underlyingTask = this.toTask()
        )
    }

    private fun Document.toExecutionSummary(
        executionId: ExecutionId
    ): ExecutionSummary {
        val task = this.toTask()
        val execution = task.execution(executionId)!!
        return ExecutionSummary(
            execution = execution,
            underlyingTask = task
        )
    }

    private fun Document.toTask(): Task {
        return Task(this)
    }

    private fun Document.toExecution(): Execution {
        return Execution(this)
    }
}
