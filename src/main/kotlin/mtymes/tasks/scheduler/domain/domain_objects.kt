package mtymes.tasks.scheduler.domain

import javafixes.`object`.Microtype
import mtymes.tasks.common.mongo.DocumentExt.getDocument
import mtymes.tasks.common.mongo.DocumentExt.getListOfDocuments
import mtymes.tasks.common.mongo.DocumentExt.getNullableDocument
import mtymes.tasks.common.mongo.DocumentExt.getNullableUTCDateTime
import mtymes.tasks.common.mongo.DocumentExt.getUTCDateTime
import mtymes.tasks.scheduler.dao.UniversalScheduler
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.CAN_BE_EXECUTED_AS_OF
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.CREATED_AT
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.DATA
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.DELETABLE_AFTER
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.EXECUTIONS
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.EXECUTION_ATTEMPTS_LEFT
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.EXECUTION_ID
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.FINISHED_AT
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.HEARTBEAT_AT
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.LAST_EXECUTION
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.MAX_EXECUTION_ATTEMPTS_COUNT
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.STARTED_AT
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.STATUS
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.STATUS_UPDATED_AT
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.SUSPENDED_AT
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.SUSPENSION_COUNT
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.TIMES_OUT_AFTER
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.UN_SUSPENDED_AT
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.UPDATED_AT
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.WAS_RETRYABLE_FAIL
import org.bson.Document
import java.time.ZonedDateTime
import java.util.*


class TaskId(value: String) : Microtype<String>(value) {
    companion object {
        fun uniqueTaskId() = TaskId(UUID.randomUUID().toString())
    }
}

class ExecutionId(value: UUID) : Microtype<UUID>(value) {
    constructor(value: String) : this(UUID.fromString(value))
}

enum class TaskStatus {
    available,
    paused,
    cancelled,
    inProgress,
    suspended,
    succeeded,
    failed,
    timedOut
}

enum class ExecutionStatus {
    running,
    cancelled,
    suspended,
    succeeded,
    failed,
    timedOut
}

data class FetchedExecutionSummary(
    val fetchedExecution: Execution,
    val wasAwokenFromSuspension: Boolean,

    val underlyingTask: Task
)

data class ExecutionSummary(
    val execution: Execution,

    val underlyingTask: Task
)

data class Task(
    private val taskDocument: Document
) {
    val taskId: TaskId = TaskId(taskDocument.getString(UniversalScheduler.TASK_ID))
    val status: TaskStatus = TaskStatus.valueOf(taskDocument.getString(STATUS))
    val executions: List<Execution> = taskDocument
        .getListOfDocuments(EXECUTIONS)
        .map { executionDoc -> Execution(executionDoc) }

    fun data(): Document {
        return taskDocument.getDocument(DATA)
    }

    fun executionIds(): List<ExecutionId> {
        return executions.map { execution -> execution.executionId }
    }

    fun lastExecutionId(): ExecutionId? {
        return taskDocument.getNullableDocument(LAST_EXECUTION)?.getString(EXECUTION_ID)?.let { ExecutionId(it) }
    }

    fun lastExecutionStatus(): ExecutionStatus? {
        return taskDocument.getNullableDocument(LAST_EXECUTION)?.getString(STATUS)?.let { ExecutionStatus.valueOf(it) }
    }

    fun lastExecutionTimesOutAfter(): ZonedDateTime? {
        return taskDocument.getNullableDocument(LAST_EXECUTION)?.getUTCDateTime(TIMES_OUT_AFTER)
    }

    fun execution(executionId: ExecutionId): Execution? {
        return executions.find { execution ->
            execution.executionId == executionId
        }
    }

    fun createdAt(): ZonedDateTime {
        return taskDocument.getUTCDateTime(CREATED_AT)
    }

    fun canBeExecutedAsOf(): ZonedDateTime {
        return taskDocument.getUTCDateTime(CAN_BE_EXECUTED_AS_OF)
    }

    fun updatedAt(): ZonedDateTime {
        return taskDocument.getUTCDateTime(UPDATED_AT)
    }

    fun statusUpdatedAt(): ZonedDateTime {
        return taskDocument.getUTCDateTime(STATUS_UPDATED_AT)
    }

    fun deletableAfter(): ZonedDateTime {
        return taskDocument.getUTCDateTime(DELETABLE_AFTER)
    }

    fun maxAttemptsCount(): Int {
        return taskDocument.getInteger(MAX_EXECUTION_ATTEMPTS_COUNT)
    }

    fun attemptsLeft(): Int {
        var attemptsLeft = taskDocument.getInteger(EXECUTION_ATTEMPTS_LEFT)
        if (status == TaskStatus.suspended) {
            attemptsLeft -= 1
        }
        return attemptsLeft
    }
}

data class Execution(
    private val executionDoc: Document
) {
    val executionId: ExecutionId = ExecutionId(executionDoc.getString(EXECUTION_ID))
    val status: ExecutionStatus = ExecutionStatus.valueOf(executionDoc.getString(STATUS))

    fun data(): Document {
        return executionDoc.getDocument(DATA)
    }

    fun startedAt(): ZonedDateTime {
        return executionDoc.getUTCDateTime(STARTED_AT)
    }

    fun updatedAt(): ZonedDateTime {
        return executionDoc.getUTCDateTime(UPDATED_AT)
    }

    fun statusUpdatedAt(): ZonedDateTime {
        return executionDoc.getUTCDateTime(STATUS_UPDATED_AT)
    }

    fun timesOutAfter(): ZonedDateTime {
        return executionDoc.getUTCDateTime(TIMES_OUT_AFTER)
    }

    fun heartBeatAt(): ZonedDateTime? {
        return executionDoc.getNullableUTCDateTime(HEARTBEAT_AT)
    }

    fun suspendedAt(): ZonedDateTime? {
        return executionDoc.getNullableUTCDateTime(SUSPENDED_AT)
    }

    fun unSuspendedAt(): ZonedDateTime? {
        return executionDoc.getNullableUTCDateTime(UN_SUSPENDED_AT)
    }

    fun suspensionCount(): Int {
        return executionDoc.getInteger(SUSPENSION_COUNT) ?: 0
    }

    fun finishedAt(): ZonedDateTime? {
        return executionDoc.getNullableUTCDateTime(FINISHED_AT)
    }

    fun wasRetryableFail(): Boolean? {
        return executionDoc.getBoolean(WAS_RETRYABLE_FAIL)
    }
}
