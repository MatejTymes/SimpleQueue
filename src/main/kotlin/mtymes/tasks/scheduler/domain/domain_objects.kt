package mtymes.tasks.scheduler.domain

import javafixes.`object`.Microtype
import mtymes.tasks.scheduler.dao.UniversalScheduler
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.DATA
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.EXECUTIONS
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.EXECUTION_ATTEMPTS_LEFT
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.EXECUTION_ID
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.LAST_EXECUTION_ID
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.MAX_EXECUTION_ATTEMPTS_COUNT
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.STATUS
import org.bson.Document
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
    paused,
    available,
    inProgress,
    suspended,
    succeeded,
    // todo: mtymes - maybe add failedAndReachedRetryLimit
    failed,
    cancelled,
    timedOut
}

enum class ExecutionStatus {
    running,
    suspended,
    succeeded,
    failed,
    cancelled,
    timedOut
}

/*
 todo: mtymes
   RuntimeConfig
   - isSuspendable: Boolean
   - recordAllExecutions: Boolean
   - afterStartKeepAliveFor: Duration
   - afterHeartBeatKeepAliveFor: Duration
 */

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
        return executions.find {
            execution -> execution.executionId == executionId
        }
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
}