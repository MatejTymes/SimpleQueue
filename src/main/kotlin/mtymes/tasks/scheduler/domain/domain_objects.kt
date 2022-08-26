package mtymes.tasks.scheduler.domain

import javafixes.`object`.Microtype
import mtymes.tasks.common.host.HostUtil.Companion.shortLocalHostName
import org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric
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

// todo: mtymes - move into generic package and reuse for distributed lock
class WorkerId(value: String) : Microtype<String>(value) {

    companion object {
        private val counter = AtomicLong(0L)

        fun uniqueWorkerId(): WorkerId {
            return WorkerId("${shortLocalHostName()}-${uniqueSuffix()}")
        }

        fun uniqueWorkerId(taskName: String): WorkerId {
            return WorkerId("${shortLocalHostName()}-${taskName}-${counter.incrementAndGet()}")
        }

        private fun uniqueSuffix(): String {
            return "${randomAlphanumeric(3)}${counter.incrementAndGet()}"
//            return counter.incrementAndGet().toString()
//            return UUID.randomUUID().toString()
//            return RandomStringUtils.randomAlphanumeric(8)
        }
    }
}

enum class TaskStatus {
    // todo: mtymes - add paused state
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