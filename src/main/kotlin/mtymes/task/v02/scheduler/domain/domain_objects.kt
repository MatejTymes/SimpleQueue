package mtymes.task.v02.scheduler.domain

import javafixes.`object`.Microtype
import mtymes.task.v02.common.host.HostUtil.Companion.shortLocalHostName
import org.apache.commons.lang3.RandomStringUtils
import org.bson.Document
import java.util.*


class TaskId(value: UUID) : Microtype<UUID>(value) {
    constructor(value: String) : this(UUID.fromString(value))

    companion object {
        fun uniqueTaskId() = TaskId(UUID.randomUUID())
    }
}

class ExecutionId(value: UUID) : Microtype<UUID>(value) {
    constructor(value: String) : this(UUID.fromString(value))
}

class WorkerId(value: String) : Microtype<String>(value) {

    companion object {
        fun uniqueWorkerId(): WorkerId {
//            return WorkerId(shortLocalHostName() + "-" + UUID.randomUUID())
            return WorkerId(shortLocalHostName() + ":" + RandomStringUtils.randomAlphanumeric(8))
        }
    }
}

enum class TaskStatus {
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

data class TaskConfig(
    val maxAttemptCount: Int
    // todo: mtymes - add isSuspendable
    // todo: mtymes - recordAllExecutions
)

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
    val status: TaskStatus
)

data class Execution(
    val executionId: ExecutionId,
    val data: Document,
    val status: ExecutionStatus
)

data class StartedExecutionSummary(
    val task: Task,
    val execution: Execution,

    val wasSuspended: Boolean
    // todo: mtymes - add lastSuspensionStage
)

data class TaskSummary(
    val task: Task,
    val executions: List<Execution>,
    val config: TaskConfig
)