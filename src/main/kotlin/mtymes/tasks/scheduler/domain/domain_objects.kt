package mtymes.tasks.scheduler.domain

import javafixes.`object`.Microtype
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

data class Task(
    val taskId: TaskId,
    val data: Document,
    val status: TaskStatus,
    val maxAttemptsCount: Int,
    val attemptsLeft: Int,
    // todo: mtymes - add isSuspendable
    // todo: mtymes - recordAllExecutions
)

data class Execution(
    val executionId: ExecutionId,
    val data: Document,
    val status: ExecutionStatus,
)

data class StartedExecutionSummary(
    val task: Task,
    val execution: Execution,

    val wasAwokenFromSuspension: Boolean
)

data class TaskSummary(
    val task: Task,
    val executions: List<Execution>,
)