package mtymes.task.v02.scheduler.domain

import javafixes.`object`.Microtype
import mtymes.task.v02.common.host.HostUtil.Companion.shortLocalHostName
import org.bson.Document
import java.util.*
import java.util.concurrent.atomic.AtomicLong


class TaskId(value: String) : Microtype<String>(value) {
    companion object {
        fun uniqueTaskId() = TaskId(UUID.randomUUID().toString())
    }
}

class ExecutionId(value: UUID) : Microtype<UUID>(value) {
    constructor(value: String) : this(UUID.fromString(value))
}

class WorkerId(value: String) : Microtype<String>(value) {

    companion object {
        private val counter = AtomicLong(0L)

        fun uniqueWorkerId(): WorkerId {
            return WorkerId(shortLocalHostName() + "-" + counter.incrementAndGet())
//            return WorkerId(shortLocalHostName() + "-" + UUID.randomUUID())
//            return WorkerId(shortLocalHostName() + ":" + RandomStringUtils.randomAlphanumeric(8))
        }

        fun uniqueWorkerId(taskName: String): WorkerId {
            return WorkerId(shortLocalHostName() + "-" + taskName + "-" + counter.incrementAndGet())
//            return WorkerId(shortLocalHostName() + "-" + UUID.randomUUID())
//            return WorkerId(shortLocalHostName() + ":" + RandomStringUtils.randomAlphanumeric(8))
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
)

data class TaskSummary(
    val task: Task,
    val executions: List<Execution>,
    val config: TaskConfig
)