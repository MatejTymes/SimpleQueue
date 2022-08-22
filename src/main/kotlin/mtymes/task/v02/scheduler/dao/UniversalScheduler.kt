package mtymes.task.v02.scheduler.dao

import com.mongodb.client.MongoCollection
import com.mongodb.client.model.FindOneAndUpdateOptions
import com.mongodb.client.model.ReturnDocument
import com.mongodb.client.result.UpdateResult
import mtymes.common.mongo.DocBuilder.Companion.emptyDoc
import mtymes.task.v02.common.mongo.DocBuilder.Companion.doc
import mtymes.task.v02.common.mongo.DocBuilder.Companion.docBuilder
import mtymes.task.v02.common.mongo.findOne
import mtymes.task.v02.common.mongo.insert
import mtymes.task.v02.common.time.Clock
import mtymes.task.v02.common.time.UTCClock
import mtymes.task.v02.scheduler.domain.*
import mtymes.task.v02.scheduler.domain.TaskId.Companion.uniqueTaskId
import mtymes.task.v02.scheduler.domain.WorkerId.Companion.uniqueWorkerId
import mtymes.task.v02.scheduler.exceptions.*
import org.bson.Document
import java.time.Duration
import java.time.ZonedDateTime
import java.util.*

// todo: mtymes - add ttl index for this
// todo: mtymes - add indexes - should be done by users of this class

// todo: mtymes - rename to GenericScheduler
// todo: mtymes - update ttl - to bigger to smaller value
class UniversalScheduler(
    val clock: Clock = UTCClock()
) {

    companion object {

        val UNIVERSAL_SCHEDULER = UniversalScheduler()

        // SHARED FIELDS

        const val DATA = "data"
        const val STATUS = "status"
        const val STATUS_UPDATED_AT = "statusUpdatedAt"
        const val LAST_UPDATED_AT = "lastUpdatedAt"

        // TASK FIELDS

        const val TASK_ID = "_id"
        const val CREATED_AT = "createdAt"
        const val DELETE_AFTER = "deleteAfter"

        const val MAX_EXECUTION_ATTEMPTS_COUNT = "maxAttemptsCount"
        const val EXECUTION_ATTEMPTS_LEFT = "attemptsLeft"

        // todo: mtymes - add field TaskConfig.isSuspendable to prevent suspension of un-suspendable tasks (and also do a runtime check if proper flag is set on fetching tasks)
        const val CAN_EXECUTE_AFTER = "canExecuteAfter"

        const val LAST_HEARTBEAT_AT = "lastHeartBeatAt"
        const val KILLABLE_AFTER = "killableAfter"
        const val LAST_EXECUTION_ID = "lastExecutionId"
        const val LAST_EXECUTION_STATE = "lastExecutionState"

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
        // todo: mtymes - shouldn't this be optional
        const val WORKER_ID = "workerId"
        const val STARTED_AT = "startadAt"
        const val SUSPENDED_AT = "suspendedAt"
        const val LAST_UN_SUSPENDED_AT = "lastUnSuspendedAt"
        const val SUSPENSION_COUNT = "suspensionCount"
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


    fun submitTask(
        coll: MongoCollection<Document>,
        config: TaskConfig,
        data: Document,
        ttlDuration: Duration,
        delayStartBy: Duration = Duration.ofSeconds(0)
    ): TaskId? {
        val now = clock.now()

        val taskId = uniqueTaskId()

        val success = coll.insert(
            doc(
                TASK_ID to taskId,
                CREATED_AT to now,
                DATA to data,
                DELETE_AFTER to now.plus(ttlDuration),
                MAX_EXECUTION_ATTEMPTS_COUNT to config.maxAttemptCount,

                EXECUTION_ATTEMPTS_LEFT to config.maxAttemptCount,
                CAN_EXECUTE_AFTER to now.plus(delayStartBy),

                STATUS to TaskStatus.available,
                STATUS_UPDATED_AT to now,

                LAST_UPDATED_AT to now,
            )
        )

        if (success) {
            return taskId
        } else {
            return null
        }
    }

    // todo: split into: startNextAvailableExecution/resumeNextAvailableSuspendedExecution/startOrResumeNextAvailableExecution
    fun fetchNextAvailableExecution(
        coll: MongoCollection<Document>,
        keepAliveForDuration: Duration,
        areTheseTasksSuspendable: Boolean,
        additionalConstraint: Document = emptyDoc(),
        workerId: WorkerId = uniqueWorkerId(),
        sortOrder: Document = doc(CAN_EXECUTE_AFTER to 1)
    ): StartedExecutionSummary? {
        if (areTheseTasksSuspendable) {
            val now = clock.now()

            val possibleTasksToFetch = coll.find(
                docBuilder()
                    .putAll(additionalConstraint)
                    .putAll(
                        STATUS to doc("\$in", listOf(TaskStatus.available, TaskStatus.suspended)),
                        CAN_EXECUTE_AFTER to doc("\$lte", now),
                        EXECUTION_ATTEMPTS_LEFT to doc("\$gte", 1)
                    )
                    .build()
            ).sort(
                sortOrder
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
                        keepAliveForDuration = keepAliveForDuration,
                        workerId = workerId,
                        taskId = Optional.of(taskId),
                        additionalQuery = additionalConstraint,
                        sortOrder = sortOrder
                    )

                    if (execution != null) {
                        return execution
                    }
                } else if (taskStatus == TaskStatus.suspended) {
                    val lastExecutionId = ExecutionId(possibleTaskToFetch.getString(LAST_EXECUTION_ID))

                    val execution = resumeSuspendedExecution(
                        coll = coll,
                        taskId = taskId,
                        lastExpectedExecutionId =  lastExecutionId,
                        keepAliveForDuration = keepAliveForDuration,
                        workerId = workerId,
                        additionalQuery = additionalConstraint
                    )

                    if (execution != null) {
                        return execution
                    }
                }
            }

            // todo: mtymes - if got some tasks, but all were actually fetched, then maybe try again

            return null
        } else {
            val execution = startNewExecution(
                coll = coll,
                keepAliveForDuration = keepAliveForDuration,
                workerId = workerId,
                taskId = Optional.empty(),
                additionalQuery = additionalConstraint,
                sortOrder = sortOrder
            )

            return execution
        }
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

    fun updateExecutionData(
        coll: MongoCollection<Document>,
        executionId: ExecutionId,
        data: Document,
        mustBeInProgress: Boolean
    ): Boolean {
        val now = clock.now()

        val result: UpdateResult = coll.updateOne(
            if (mustBeInProgress) {
                queryForExecution(
                    executionId,
                    TaskStatus.inProgress,
                    ExecutionStatus.running
                )
            } else {
                doc(
                    LAST_EXECUTION_ID to executionId,
                    EXECUTIONS to doc("\$elemMatch" to doc(EXECUTION_ID to executionId))
                )
            },
            doc(
                "\$set" to docBuilder()
                    .putAll(
                        EXECUTIONS + ".\$." + LAST_UPDATED_AT to now,
                        LAST_UPDATED_AT to now
                    )
                    .putAll(
                        data.mapKeys { entry ->
                            EXECUTIONS + ".\$." + DATA + "." + entry.key
                        }
                    )
                    .build(),
            )
        )

        return result.matchedCount > 0
    }

    // todo: mtymes - add field affectsLastUpdatesAt
    fun registerHeartBeat(
        coll: MongoCollection<Document>,
        executionId: ExecutionId,
        keepAliveForDuration: Duration
    ): Boolean {
        val now = clock.now()
        val keepAliveUntil = now.plus(keepAliveForDuration)

        val result = coll.updateOne(
            queryForExecution(
                executionId,
                TaskStatus.inProgress,
                ExecutionStatus.running
            ),
            doc(
                "\$set" to doc(
                    LAST_HEARTBEAT_AT to now,
                    KILLABLE_AFTER to keepAliveUntil,
                )
            )
        )

        return result.modifiedCount > 0
    }

    fun markAsSucceeded(
        coll: MongoCollection<Document>,
        executionId: ExecutionId,
        executionDataChanges: Document = emptyDoc()
    ) {
        val now = clock.now()

        updateExecution(
            coll = coll,
            executionId = executionId,
            fromTaskStatus = TaskStatus.inProgress,
            fromExecutionStatus = ExecutionStatus.running,
            toTaskStatus = TaskStatus.succeeded,
            toExecutionStatus = ExecutionStatus.succeeded,
            now = now,
            customTaskUpdates = emptyDoc(),
            customExecutionUpdates = doc(
                FINISHED_AT to now
            ),
            executionDataChanges = executionDataChanges
        )
    }

    fun markAsFailedButCanRetry(
        coll: MongoCollection<Document>,
        executionId: ExecutionId,
        retryDelayDuration: Duration,
        executionDataChanges: Document = emptyDoc()
    ) {
        val now = clock.now()

        updateExecution(
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
                CAN_EXECUTE_AFTER to doc(
                    "\$cond" to listOf(
                        doc("\$gt" to listOf("\$" + EXECUTION_ATTEMPTS_LEFT, 0)),
                        now.plus(retryDelayDuration),
                        "\$" + CAN_EXECUTE_AFTER
                    )
                )
            ),
            customExecutionUpdates = doc(
                FINISHED_AT to now
            ),
            executionDataChanges = executionDataChanges
        )
    }

    fun markAsFailedButCanNOTRetry(
        coll: MongoCollection<Document>,
        executionId: ExecutionId,
        executionDataChanges: Document = emptyDoc()
    ) {
        val now = clock.now()

        updateExecution(
            coll = coll,
            executionId = executionId,
            fromTaskStatus = TaskStatus.inProgress,
            fromExecutionStatus = ExecutionStatus.running,
            toTaskStatus = ToSingleTaskStatus(TaskStatus.failed),
            toExecutionStatus = ExecutionStatus.failed,
            now = now,
            customTaskUpdates = emptyDoc(),
            customExecutionUpdates = doc(
                FINISHED_AT to now
            ),
            executionDataChanges = executionDataChanges
        )
    }

    fun markAsCancelled(
        coll: MongoCollection<Document>,
        executionId: ExecutionId,
        executionDataChanges: Document = emptyDoc()
    ) {
        val now = clock.now()

        updateExecution(
            coll = coll,
            executionId = executionId,
            fromTaskStatus = TaskStatus.inProgress,
            fromExecutionStatus = ExecutionStatus.running,
            toTaskStatus = TaskStatus.cancelled,
            toExecutionStatus = ExecutionStatus.cancelled,
            now = now,
            customTaskUpdates = emptyDoc(),
            customExecutionUpdates = doc(
                FINISHED_AT to now
            ),
            executionDataChanges = executionDataChanges
        )
    }

    fun markTaskAsCancelled(
        coll: MongoCollection<Document>,
        taskId: TaskId
    ) {
        val now = clock.now()

        coll.findOneAndUpdate(
            doc(
                TASK_ID to taskId,
                STATUS to TaskStatus.available
            ),
            doc(
                "\$set" to doc(
                    STATUS to TaskStatus.cancelled,
                    STATUS_UPDATED_AT to now,
                    LAST_UPDATED_AT to now
                )
            )
        )
    }

    // todo: mtymes - add suspension phase
    // todo: mtymes - fail if suspending non-suspendable task
    fun markAsSuspended(
        coll: MongoCollection<Document>,
        executionId: ExecutionId,
        retryDelayDuration: Duration,
        executionDataChanges: Document = emptyDoc()
    ) {
        val now = clock.now()

        updateExecution(
            coll = coll,
            executionId = executionId,
            fromTaskStatus = TaskStatus.inProgress,
            fromExecutionStatus = ExecutionStatus.running,
            toTaskStatus = TaskStatus.suspended,
            toExecutionStatus = ExecutionStatus.suspended,
            now = now,
            customTaskUpdates = doc(
                CAN_EXECUTE_AFTER to now.plus(retryDelayDuration),

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
            executionDataChanges = executionDataChanges
        )
    }

    fun markTasksWithoutHeartBeatAsTimedOut(
        coll: MongoCollection<Document>,
        retryDelayDuration: Duration?,
        executionDataChanges: Document = emptyDoc()
    ) {
        val deadTasks: List<Document> = coll.find(
            doc(
                STATUS to TaskStatus.inProgress,
                LAST_EXECUTION_STATE to ExecutionStatus.running,
                KILLABLE_AFTER to (doc("\$lt" to clock.now())),
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
                        doc(CAN_EXECUTE_AFTER to now.plus(retryDelayDuration))
                    } else {
                        emptyDoc()
                    },
                    customExecutionUpdates = doc(
                        FINISHED_AT to now
                    ),
                    executionDataChanges = executionDataChanges
                )
            } catch (e: Exception) {
                TODO("todo: mtymes - log")
            }
        }
    }

    private fun startNewExecution(
        coll: MongoCollection<Document>,
        keepAliveForDuration: Duration,
        workerId: WorkerId,
        taskId: Optional<TaskId>,
        additionalQuery: Document = emptyDoc(),
        sortOrder: Document
    ): StartedExecutionSummary? {
        val now = clock.now()
        val keepAliveUntil = now.plus(keepAliveForDuration)

        val executionId = ExecutionId(UUID.randomUUID())

        val modifiedTask = coll.findOneAndUpdate(
            docBuilder()
                .putAll(additionalQuery)
                .putAll(
                    TASK_ID to taskId,
                    STATUS to TaskStatus.available,
                    CAN_EXECUTE_AFTER to doc("\$lte", now),
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
                        LAST_UPDATED_AT to now,
                    )
                ),
                "\$set" to doc(
                    STATUS to TaskStatus.inProgress,
                    STATUS_UPDATED_AT to now,
                    LAST_EXECUTION_ID to executionId,
                    LAST_EXECUTION_STATE to ExecutionStatus.running,
                    LAST_HEARTBEAT_AT to now,
                    KILLABLE_AFTER to keepAliveUntil,
                    LAST_UPDATED_AT to now,
                ),
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
        taskId: TaskId,
        lastExpectedExecutionId: ExecutionId,
        keepAliveForDuration: Duration,
        workerId: WorkerId,
        additionalQuery: Document = emptyDoc()
    ): StartedExecutionSummary? {
        val now = clock.now()
        val keepAliveUntil = now.plus(keepAliveForDuration)

        val modifiedTask = coll.findOneAndUpdate(
            docBuilder()
                .putAll(additionalQuery)
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
                    CAN_EXECUTE_AFTER to doc("\$lte", now),
                    EXECUTION_ATTEMPTS_LEFT to doc("\$gte", 1)
                )
                .build(),
            doc(
                "\$set" to doc(
                    STATUS to TaskStatus.inProgress,
                    STATUS_UPDATED_AT to now,
                    LAST_EXECUTION_STATE to ExecutionStatus.running,
                    LAST_HEARTBEAT_AT to now,
                    KILLABLE_AFTER to keepAliveUntil,
                    EXECUTIONS + ".\$." + STATUS to ExecutionStatus.running,
                    EXECUTIONS + ".\$." + STATUS_UPDATED_AT to now,
                    EXECUTIONS + ".\$." + LAST_UN_SUSPENDED_AT to now,
                    EXECUTIONS + ".\$." + WORKER_ID to workerId,
                    EXECUTIONS + ".\$." + LAST_UPDATED_AT to now,
                    LAST_UPDATED_AT to now,
                ),
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
        customTaskUpdates: Document,
        customExecutionUpdates: Document,
        executionDataChanges: Document // todo: mtymes - start using this
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
                                                    LAST_UPDATED_AT to now
                                                )
                                                .putAll(customExecutionUpdates)
                                                .let {
                                                    if (executionDataChanges.isEmpty()) {
                                                        it
                                                    } else {
                                                        it.put(
                                                            DATA to doc(
                                                                "\$mergeObjects" to listOf(
                                                                    "\$\$ex.data",
                                                                    executionDataChanges
                                                                )
                                                            )
                                                        )
                                                    }
                                                }
                                                .build()
                                        )
                                    ),
                                    "\$\$ex"
                                )
                            )
                        )
                    ),
                    LAST_UPDATED_AT to now
                )
                .putAll(customTaskUpdates)
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
                throw NotLastExecutionException("Execution '${executionId}' for Task '${taskId}' is NOT LAST execution (Execution '${lastExecutionId}' is)")
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
                        "Expected '${fromTaskStatus}' Task and '${fromExecutionStatus}' Execution, but got '${currentTaskStatus}' Task and '${currentExecutionStatus}' Execution instead"
                    )
                }
            }

            throw UnknownFailureReasonException("Not sure why Execution '${executionId}' was not marked as succeeded")
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
        customTaskUpdates: Document,
        customExecutionUpdates: Document,
        executionDataChanges: Document
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
            executionDataChanges = executionDataChanges
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
                },
            TaskConfig(
                maxAttemptCount = taskDoc.getInteger(MAX_EXECUTION_ATTEMPTS_COUNT)
            )
        )
    }

    private fun documentToTask(taskDoc: Document) = Task(
        taskId = TaskId(taskDoc.getString(TASK_ID)),
        data = taskDoc.get(DATA) as Document,
        status = TaskStatus.valueOf(taskDoc.getString(STATUS))
    )

    private fun documentToExecution(executionDoc: Document) = Execution(
        executionId = ExecutionId(executionDoc.getString(EXECUTION_ID)),
        data = executionDoc.get(DATA) as Document,
        status = ExecutionStatus.valueOf(executionDoc.getString(STATUS))
    )
}