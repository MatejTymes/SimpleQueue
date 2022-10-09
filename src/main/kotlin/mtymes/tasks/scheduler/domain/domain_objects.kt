package mtymes.tasks.scheduler.domain

import javafixes.`object`.Microtype
import mtymes.tasks.common.mongo.DocBuilder.Companion.emptyDoc
import mtymes.tasks.common.mongo.DocumentExt.getDocument
import mtymes.tasks.common.mongo.DocumentExt.getNullableDocument
import mtymes.tasks.common.mongo.DocumentExt.getNullableListOfDocuments
import mtymes.tasks.common.mongo.DocumentExt.getNullableZonedDateTime
import mtymes.tasks.common.mongo.DocumentExt.getZonedDateTime
import mtymes.tasks.scheduler.dao.UniversalScheduler
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.CAN_BE_EXECUTED_AS_OF
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.CREATED_AT
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.DATA
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.DELETABLE_AFTER
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.EXECUTION_ATTEMPTS_LEFT
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.EXECUTION_ID
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.FINISHED_AT
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.HEARTBEAT_AT
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.KILLABLE_AFTER
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.LAST_EXECUTION
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.MAX_EXECUTIONS_COUNT
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.PREVIOUS_EXECUTIONS
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.STARTED_AT
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.STATUS
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.STATUS_UPDATED_AT
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.SUSPENDED_AT
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.SUSPENSION_COUNT
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

enum class TaskStatus(
    val isFinalStatus: Boolean = false
) {
    available,
    paused,
    running,
    suspended,
    cancelled(true),
    succeeded(true),
    failed(true),
    // so far this is the best name from these: interrupted/timedOut/killed/dead/died/terminated/aborted/timedOut
    died(true);

    companion object {
        val NON_FINAL_STATUSES = TaskStatus.values().filter { !it.isFinalStatus }.toList()
        val FINAL_STATUSES = TaskStatus.values().filter { !it.isFinalStatus }.toList()
    }
}

enum class ExecutionStatus(
    val isFinalStatus: Boolean = false
) {
    running,
    suspended,
    cancelled(true),
    succeeded(true),
    failed(true),
    // so far this is the best name from these: interrupted/timedOut/killed/dead/died/terminated/aborted/timedOut
    died(true);

    companion object {
        val NON_FINAL_STATUSES = ExecutionStatus.values().filter { !it.isFinalStatus }.toList()
        val FINAL_STATUSES = ExecutionStatus.values().filter { !it.isFinalStatus }.toList()
    }
}

data class Task(
    private val taskDocument: Document
) {
    val taskId: TaskId = TaskId(taskDocument.getString(UniversalScheduler.TASK_ID))
    val status: TaskStatus = TaskStatus.valueOf(taskDocument.getString(STATUS))
    val previousExecutions: List<Execution> = taskDocument
        .getNullableListOfDocuments(PREVIOUS_EXECUTIONS)
        ?.map { executionDoc -> Execution(executionDoc, false) }
        ?: emptyList()
    val lastExecution: Execution? = taskDocument.getNullableDocument(LAST_EXECUTION)
        ?.let { executionDoc -> Execution(executionDoc, true) }
    val allExecutions: List<Execution> = if (lastExecution == null) {
        previousExecutions
    } else {
        previousExecutions.plus(lastExecution)
    }

    fun data(): Document {
        return taskDocument.getDocument(DATA)
    }

    fun executionIds(): List<ExecutionId> {
        return allExecutions.map { execution -> execution.executionId }
    }

    fun execution(executionId: ExecutionId): Execution? {
        return allExecutions.find { execution ->
            execution.executionId == executionId
        }
    }

    fun createdAt(): ZonedDateTime {
        return taskDocument.getZonedDateTime(CREATED_AT)
    }

    fun canBeExecutedAsOf(): ZonedDateTime {
        return taskDocument.getZonedDateTime(CAN_BE_EXECUTED_AS_OF)
    }

    fun updatedAt(): ZonedDateTime {
        return taskDocument.getZonedDateTime(UPDATED_AT)
    }

    fun statusUpdatedAt(): ZonedDateTime {
        return taskDocument.getZonedDateTime(STATUS_UPDATED_AT)
    }

    fun deletableAfter(): ZonedDateTime {
        return taskDocument.getZonedDateTime(DELETABLE_AFTER)
    }

    fun maxExecutionsCount(): Int {
        return taskDocument.getInteger(MAX_EXECUTIONS_COUNT)
    }

    fun executionAttemptsLeft(): Int {
        var attemptsLeft = taskDocument.getInteger(EXECUTION_ATTEMPTS_LEFT)
        if (status == TaskStatus.suspended) {
            attemptsLeft -= 1
        }
        return attemptsLeft
    }

    fun executionsCount(): Int {
        return taskDocument.getInteger(UniversalScheduler.EXECUTIONS_COUNT)
    }
}

data class Execution(
    private val executionDoc: Document,
    val isLastExecution: Boolean
) {
    val executionId: ExecutionId = ExecutionId(executionDoc.getString(EXECUTION_ID))
    val status: ExecutionStatus = ExecutionStatus.valueOf(executionDoc.getString(STATUS))

    fun data(): Document {
        return executionDoc.getNullableDocument(DATA) ?: emptyDoc()
    }

    fun startedAt(): ZonedDateTime {
        return executionDoc.getZonedDateTime(STARTED_AT)
    }

    fun updatedAt(): ZonedDateTime {
        return executionDoc.getZonedDateTime(UPDATED_AT)
    }

    fun statusUpdatedAt(): ZonedDateTime {
        return executionDoc.getZonedDateTime(STATUS_UPDATED_AT)
    }

    fun diesAfter(): ZonedDateTime {
        return executionDoc.getZonedDateTime(KILLABLE_AFTER)
    }

    fun heartBeatAt(): ZonedDateTime? {
        return executionDoc.getNullableZonedDateTime(HEARTBEAT_AT)
    }

    fun suspendedAt(): ZonedDateTime? {
        return executionDoc.getNullableZonedDateTime(SUSPENDED_AT)
    }

    fun unSuspendedAt(): ZonedDateTime? {
        return executionDoc.getNullableZonedDateTime(UN_SUSPENDED_AT)
    }

    fun suspensionCount(): Int {
        return executionDoc.getInteger(SUSPENSION_COUNT) ?: 0
    }

    fun finishedAt(): ZonedDateTime? {
        return executionDoc.getNullableZonedDateTime(FINISHED_AT)
    }

    fun wasRetryableFail(): Boolean? {
        return executionDoc.getBoolean(WAS_RETRYABLE_FAIL)
    }
}

data class ExecutionSummary(
    val execution: Execution,

    val underlyingTask: Task
)

data class FetchedExecutionSummary(
    val fetchedExecution: Execution,
    val wasAwokenFromSuspension: Boolean,

    val underlyingTask: Task
)


sealed interface HeartBeatOutcome
object HeartBeatApplied : HeartBeatOutcome
object NoExecutionFoundToApplyHeartBeatTo : HeartBeatOutcome
data class HeartBeatNotApplied(
    val currentTaskStatus: TaskStatus,
    val currentLastExecutionId: ExecutionId,
    val currentExecutionStatus: ExecutionStatus
) : HeartBeatOutcome