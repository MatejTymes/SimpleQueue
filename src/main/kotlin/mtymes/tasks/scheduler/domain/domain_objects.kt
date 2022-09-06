package mtymes.tasks.scheduler.domain

import javafixes.`object`.Microtype
import mtymes.tasks.common.mongo.DocumentExt.toNullableUTCDateTime
import mtymes.tasks.common.mongo.DocumentExt.toUTCDateTime
import mtymes.tasks.scheduler.dao.UniversalScheduler
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.CREATED_AT
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.DATA
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.DELETABLE_AFTER
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.EXECUTIONS
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.EXECUTION_ATTEMPTS_LEFT
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.EXECUTION_ID
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.FINISHED_AT
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.HEARTBEAT_AT
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.LAST_EXECUTION_ID
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.MAX_EXECUTION_ATTEMPTS_COUNT
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.STARTED_AT
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.STATUS
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.STATUS_UPDATED_AT
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.SUSPENDED_AT
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.SUSPENSION_COUNT
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.UN_SUSPENDED_AT
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.UPDATED_AT
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

/*
* TODO: mtymes - define new status hierarchy
*
* TaskStatus | ExecutionStatus | ExecFinalStatus |   ExpectedStatus  |WasSuccess | CanBeRetried |
* -----------+-----------------+-----------------+-------------------+-----------+--------------+
*  available |                 |                 |                   |           |              |
*     paused |                 |                 |                   |           |              |
*  cancelled |                 |                 |                   |           |              |
* inProgress |     running     |                 |                   |           |              |
*  suspended |    suspended    |                 |                   |           |              |
*   finished |     finished    |    Succeeded    |       running     |   yes     |              |
*   finished |       -||-      |      Failed     |       running     |    no     |      no      |
*   finished |       -||-      |     TimedOut    | running/suspended |    no     |      no      |
*   finished |       -||-      |    Cancelled    |       running     |    no     |      no      |
*  available |       -||-      |      Failed     |       running     |    no     |     yes      |
*  available |       -||-      |     TimedOut    | running/suspended |    no     |     yes      |
* -----------+-----------------+-----------------+-------------------+-----------+--------------+
* */


enum class TaskStatus {
    available,
    paused,
    inProgress,
    suspended,
    succeeded,
    failed,

    // todo: mtymes - maybe add failedAndReachedRetryLimit
    cancelled,
    timedOut
}

enum class ExecutionStatus {
    running,
    suspended,
    succeeded,
    failed,

    // todo: mtymes - maybe add failedUnRetriably
    cancelled,
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
        .getList(EXECUTIONS, Document::class.java)
        .map { executionDoc -> Execution(executionDoc) }

    fun data(): Document {
        return taskDocument.get(DATA) as Document
    }

    fun executionIds(): List<ExecutionId> {
        return executions.map { execution -> execution.executionId }
    }

    fun lastExecutionId(): ExecutionId? {
        return taskDocument.getString(LAST_EXECUTION_ID)?.let { ExecutionId(it) }
    }

    fun execution(executionId: ExecutionId): Execution? {
        return executions.find { execution ->
            execution.executionId == executionId
        }
    }

    fun createdAt(): ZonedDateTime {
        return taskDocument.toUTCDateTime(CREATED_AT)
    }

    fun updatedAt(): ZonedDateTime {
        return taskDocument.toUTCDateTime(UPDATED_AT)
    }

    fun statusUpdatedAt(): ZonedDateTime {
        return taskDocument.toUTCDateTime(STATUS_UPDATED_AT)
    }

    fun deletableAfter(): ZonedDateTime {
        return taskDocument.toUTCDateTime(DELETABLE_AFTER)
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
        return executionDoc.get(DATA) as Document
    }

    fun startedAt(): ZonedDateTime {
        return executionDoc.toUTCDateTime(STARTED_AT)
    }

    fun updatedAt(): ZonedDateTime {
        return executionDoc.toUTCDateTime(UPDATED_AT)
    }

    fun statusUpdatedAt(): ZonedDateTime {
        return executionDoc.toUTCDateTime(STATUS_UPDATED_AT)
    }

    fun heartBeatAt(): ZonedDateTime? {
        return executionDoc.toNullableUTCDateTime(HEARTBEAT_AT)
    }

    fun suspendedAt(): ZonedDateTime? {
        return executionDoc.toNullableUTCDateTime(SUSPENDED_AT)
    }

    fun unSuspendedAt(): ZonedDateTime? {
        return executionDoc.toNullableUTCDateTime(UN_SUSPENDED_AT)
    }

    fun suspensionCount(): Int {
        return executionDoc.getInteger(SUSPENSION_COUNT) ?: 0
    }

    fun finishedAt(): ZonedDateTime? {
        return executionDoc.toNullableUTCDateTime(FINISHED_AT)
    }
}
