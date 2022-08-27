package mtymes.tasks.scheduler.dao

import com.mongodb.client.MongoCollection
import com.mongodb.client.model.FindOneAndUpdateOptions
import com.mongodb.client.model.ReturnDocument
import mtymes.tasks.common.check.ValidityChecks.expectNonEmptyDocument
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
import mtymes.tasks.scheduler.exceptions.*
import org.bson.Document
import java.time.Duration
import java.time.ZonedDateTime
import java.util.*

// todo: mtymes - add ability to provide custom ExecutionId
// todo: mtymes - ability to fail if Execution is already in wanted state (e.g. failed -> failed, cancelled -> cancelled, succeeded -> succeeded, suspended -> suspended)

// todo: mtymes - update ttl - to bigger to smaller value
// todo: mtymes - update ttl on final state
// todo: mtymes - add indexes - should be done by users of this class (e.g.: ttl index, unique executionId index, ...)
class UniversalScheduler(
    val clock: Clock = UTCClock()
) {

    companion object {

        val UNIVERSAL_SCHEDULER = UniversalScheduler()

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

        // todo: mtymes - add field TaskConfig.isSuspendable to prevent suspension of un-suspendable tasks (and also do a runtime check if proper flag is set on fetching tasks)

        const val CAN_BE_EXECUTED_AS_OF = "canBeExecutedAsOf"

        const val LAST_EXECUTION_ID = "lastExecutionId"
        const val LAST_EXECUTION_STATE = "lastExecutionState"
        const val LAST_EXECUTION_TIMES_OUT_AFTER = "lastExecutionTimesOutAfter"

        // todo: mtymes - add flag - retainOnlyLastExecution

        // when recording all executions
        const val EXECUTIONS = "executions"

        // when recording only last execution
        // todo: mtymes - add this field for retainOnlyLasExecution
//        const val EXECUTION_COUNT = "executionCount"
        // todo: mtymes - add this field for retainOnlyLasExecution
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

                STATUS to TaskStatus.available,
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
    ): StartedExecutionSummary? {
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
                        sortOrder = usedSortOrder
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
                        additionalConstraints = additionalConstraints
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
                sortOrder = usedSortOrder
            )

            return execution
        }
    }

    fun markAsSucceeded(
        coll: MongoCollection<Document>,
        executionId: ExecutionId,
        additionalTaskData: Document? = null,
        additionalExecutionData: Document? = null
    ): Document? {
        val now = clock.now()

        return updateExecution(
            coll = coll,
            executionId = executionId,
            fromTaskStatus = TaskStatus.inProgress,
            fromExecutionStatus = ExecutionStatus.running,
            toTaskStatus = TaskStatus.succeeded,
            toExecutionStatus = ExecutionStatus.succeeded,
            now = now,
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
    ): Document? {
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
            customTaskUpdates = doc(
                CAN_BE_EXECUTED_AS_OF to doc(
                    "\$cond" to listOf(
                        doc("\$gt" to listOf("\$" + EXECUTION_ATTEMPTS_LEFT, 0)),
                        now.plus(options.retryDelay),
                        "\$" + CAN_BE_EXECUTED_AS_OF
                    )
                )
            ),
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
        additionalTaskData: Document? = null,
        additionalExecutionData: Document? = null
    ): Document? {
        val now = clock.now()

        return updateExecution(
            coll = coll,
            executionId = executionId,
            fromTaskStatus = TaskStatus.inProgress,
            fromExecutionStatus = ExecutionStatus.running,
            toTaskStatus = ToSingleTaskStatus(TaskStatus.failed),
            toExecutionStatus = ExecutionStatus.failed,
            now = now,
            customExecutionUpdates = doc(
                FINISHED_AT to now
            ),
            additionalTaskData = additionalTaskData,
            additionalExecutionData = additionalExecutionData
        )
    }

    fun markAsCancelled(
        coll: MongoCollection<Document>,
        executionId: ExecutionId,
        additionalTaskData: Document? = null,
        additionalExecutionData: Document? = null
    ): Document? {
        val now = clock.now()

        return updateExecution(
            coll = coll,
            executionId = executionId,
            fromTaskStatus = TaskStatus.inProgress,
            fromExecutionStatus = ExecutionStatus.running,
            toTaskStatus = TaskStatus.cancelled,
            toExecutionStatus = ExecutionStatus.cancelled,
            now = now,
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
        additionalTaskData: Document? = null
    ): Document? {
        val now = clock.now()

        val fromTaskStatus = TaskStatus.available
        val toTaskStatus = TaskStatus.cancelled

        val modifiedTask = coll.findOneAndUpdate(
            doc(
                TASK_ID to taskId,
                STATUS to fromTaskStatus
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
                    .build()
            ),
            FindOneAndUpdateOptions()
                .returnDocument(ReturnDocument.AFTER)
        )

        if (modifiedTask == null) {
            val task: Document? = coll.findOne(
                doc(TASK_ID to taskId)
            )

            if (task == null) {
                throw ExecutionNotFoundException("Task '${taskId}' NOT FOUND")
            }

            val currentTaskStatus = TaskStatus.valueOf(task.getString(STATUS))
            if (currentTaskStatus != fromTaskStatus) {
                throw UnexpectedStatusException(
                    "Failed to mark Task '${taskId}' as '${toTaskStatus}' as expected '${fromTaskStatus}' but got '${currentTaskStatus}' Task status instead"
                )
            }

            throw UnknownFailureReasonException(
                "Not sure why Task '${taskId}' was not marked as ${toTaskStatus}"
            )
        }

        return modifiedTask
    }

    fun markAsSuspended(
        coll: MongoCollection<Document>,
        executionId: ExecutionId,
        options: MarkAsSuspendedOptions,
        additionalTaskData: Document? = null,
        additionalExecutionData: Document? = null
    ): Document? {
        val now = clock.now()

        return updateExecution(
            coll = coll,
            executionId = executionId,
            fromTaskStatus = TaskStatus.inProgress,
            fromExecutionStatus = ExecutionStatus.running,
            toTaskStatus = TaskStatus.suspended,
            toExecutionStatus = ExecutionStatus.suspended,
            now = now,
            customTaskUpdates = doc(
                CAN_BE_EXECUTED_AS_OF to now.plus(options.suspendFor),

                // $inc - section
                // todo: mtymes - handle differently
                EXECUTION_ATTEMPTS_LEFT to doc("\$sum" to listOf("\$" + EXECUTION_ATTEMPTS_LEFT, 1))
            ),
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
                STATUS to TaskStatus.inProgress,
                LAST_EXECUTION_STATE to ExecutionStatus.running,
                LAST_EXECUTION_TIMES_OUT_AFTER to (doc("\$lt" to clock.now())),
            )
        ).toList()

        for (deadTask: Document in deadTasks) {
            try {
                val lastExecutionId = ExecutionId(deadTask.getString(LAST_EXECUTION_ID))
                val executionAttemptsLeft = deadTask.getInteger(EXECUTION_ATTEMPTS_LEFT)

                val now = clock.now()

                updateExecution(
                    coll = coll,
                    executionId = lastExecutionId,
                    fromTaskStatus = TaskStatus.inProgress,
                    fromExecutionStatus = ExecutionStatus.running,
                    toTaskStatus = if (executionAttemptsLeft > 0) TaskStatus.available else TaskStatus.timedOut,
                    toExecutionStatus = ExecutionStatus.timedOut,
                    now = now,
                    customTaskUpdates = if (executionAttemptsLeft > 0) {
                        doc(CAN_BE_EXECUTED_AS_OF to now.plus(options.retryDelay))
                    } else {
                        null
                    },
                    customExecutionUpdates = doc(
                        FINISHED_AT to now
                    ),
                    additionalTaskData = additionalTaskData,
                    additionalExecutionData = additionalExecutionData
                )
            } catch (e: Exception) {
                TODO("todo: mtymes - log")
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
                    .build()
            )
        )

        return result.modifiedCount > 0
    }

    // todo: mtymes - maybe add custom query criteria
    fun updateTaskData(
        coll: MongoCollection<Document>,
        taskId: TaskId,
        additionalTaskData: Document
    ): Document? {
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
                    .build()
            ),
            FindOneAndUpdateOptions()
                .returnDocument(ReturnDocument.AFTER)
        )

        return result
    }

    // todo: mtymes - maybe add custom query criteria
    fun updateExecutionData(
        coll: MongoCollection<Document>,
        executionId: ExecutionId,
        options: UpdateExecutionDataOptions,
        additionalExecutionData: Document,
        additionalTaskData: Document? = null
    ): Document? {
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
                    .build(),
            ),
            FindOneAndUpdateOptions()
                .returnDocument(ReturnDocument.AFTER)
        )

        return result
    }

    fun getTaskSummary(
        coll: MongoCollection<Document>,
        taskId: TaskId
    ): TaskSummary? {
        return coll.findOne(
            doc(
                TASK_ID to taskId
            )
        )?.let { taskDoc ->
            documentToTakSummary(taskDoc)
        }
    }

    private fun startNewExecution(
        coll: MongoCollection<Document>,
        workerId: WorkerId,
        taskId: Optional<TaskId>,
        keepAliveFor: Duration,
        additionalConstraints: Document?,
        sortOrder: Document
    ): StartedExecutionSummary? {
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
                "\$set" to doc(
                    STATUS to TaskStatus.inProgress,
                    STATUS_UPDATED_AT to now,
                    LAST_EXECUTION_ID to executionId,
                    LAST_EXECUTION_STATE to ExecutionStatus.running,
//                    LAST_HEARTBEAT_AT to now,
                    LAST_EXECUTION_TIMES_OUT_AFTER to keepAliveUntil,
                    UPDATED_AT to now,
                ),
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

        return modifiedTask?.let { taskDoc ->
            return documentToStartedExecutionSummary(
                executionId,
                taskDoc,
                false
            )
        }
    }

    private fun resumeSuspendedExecution(
        coll: MongoCollection<Document>,
        workerId: WorkerId,
        taskId: TaskId,
        lastExpectedExecutionId: ExecutionId,
        keepAliveFor: Duration,
        additionalConstraints: Document?
    ): StartedExecutionSummary? {
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
                    LAST_EXECUTION_STATE to ExecutionStatus.suspended,
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
                "\$set" to doc(
                    STATUS to TaskStatus.inProgress,
                    STATUS_UPDATED_AT to now,
                    LAST_EXECUTION_STATE to ExecutionStatus.running,
//                    LAST_HEARTBEAT_AT to now,
                    LAST_EXECUTION_TIMES_OUT_AFTER to keepAliveUntil,
                    EXECUTIONS + ".\$." + STATUS to ExecutionStatus.running,
                    EXECUTIONS + ".\$." + STATUS_UPDATED_AT to now,
                    EXECUTIONS + ".\$." + LAST_UN_SUSPENDED_AT to now,
                    EXECUTIONS + ".\$." + WORKER_ID to workerId,
                    EXECUTIONS + ".\$." + TIMES_OUT_AFTER to keepAliveUntil,
                    EXECUTIONS + ".\$." + UPDATED_AT to now,
                    UPDATED_AT to now,
                ),
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

        return modifiedTask?.let { taskDoc ->
            return documentToStartedExecutionSummary(
                lastExpectedExecutionId,
                taskDoc,
                true
            )
        }
    }

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
    ): Document? {
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
                    LAST_EXECUTION_STATE to toExecutionStatus,
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
            return modifiedTask
        } else {
            val task: Document? = coll.findOne(
                doc(EXECUTIONS + "." + EXECUTION_ID, executionId)
            )

            if (task == null) {
                throw ExecutionNotFoundException("Execution '${executionId}' NOT FOUND")
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
                    return null
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
    ): Document? {
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

    private fun documentToStartedExecutionSummary(
        expectedLastExecutionId: ExecutionId,
        taskDoc: Document,
        wasSuspended: Boolean
    ): StartedExecutionSummary {
        val lastExecutionId = ExecutionId(taskDoc.getString(LAST_EXECUTION_ID))

        if (expectedLastExecutionId != lastExecutionId) {
            throw ExecutionSupersededByAnotherOneException(
                "No idea how this happened but our started Execution '${expectedLastExecutionId}' was superseded by Execution '${lastExecutionId}'"
            )
        }

        val executionDoc = taskDoc
            .getList(EXECUTIONS, Document::class.java)
            .first { ExecutionId(it.getString(EXECUTION_ID)) == lastExecutionId }

        return StartedExecutionSummary(
            documentToTask(taskDoc),
            documentToExecution(executionDoc),
            wasSuspended
        )
    }

    private fun documentToTakSummary(
        taskDoc: Document
    ): TaskSummary {
        return TaskSummary(
            documentToTask(taskDoc),
            taskDoc
                .getList(EXECUTIONS, Document::class.java)
                .map { executionDoc ->
                    documentToExecution(executionDoc)
                }
        )
    }

    private fun documentToTask(taskDoc: Document) = Task(
        taskId = TaskId(taskDoc.getString(TASK_ID)),
        data = taskDoc.get(DATA) as Document,
        status = TaskStatus.valueOf(taskDoc.getString(STATUS)),
        maxAttemptsCount = taskDoc.getInteger(MAX_EXECUTION_ATTEMPTS_COUNT),
        attemptsLeft = taskDoc.getInteger(EXECUTION_ATTEMPTS_LEFT)
    )

    private fun documentToExecution(executionDoc: Document) = Execution(
        executionId = ExecutionId(executionDoc.getString(EXECUTION_ID)),
        data = executionDoc.get(DATA) as Document,
        status = ExecutionStatus.valueOf(executionDoc.getString(STATUS))
    )
}
