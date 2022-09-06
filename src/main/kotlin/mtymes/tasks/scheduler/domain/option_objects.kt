package mtymes.tasks.scheduler.domain

import mtymes.tasks.common.check.ValidityChecks.expectAtLeastOne
import mtymes.tasks.common.check.ValidityChecks.expectNonNegativeDuration
import mtymes.tasks.common.check.ValidityChecks.expectNullOrNonNegativeDuration
import mtymes.tasks.common.check.ValidityChecks.expectNullOrPositiveDuration
import mtymes.tasks.common.check.ValidityChecks.expectPositiveDuration
import mtymes.tasks.common.time.Durations.ZERO_SECONDS
import mtymes.tasks.scheduler.domain.TaskId.Companion.uniqueTaskId
import java.time.Duration

data class SubmitTaskOptions(
    val taskIdGenerator: (() -> TaskId) = { uniqueTaskId() },
    val maxAttemptsCount: Int = 1,
    val ttl: Duration,
    val delayStartBy: Duration = ZERO_SECONDS,
    val submitAsPaused: Boolean = false

) {
    init {
        expectAtLeastOne("maxAttemptsCount", maxAttemptsCount)
        expectPositiveDuration("ttl", ttl)
        expectNonNegativeDuration("delayStartBy", delayStartBy)
    }
}

data class FetchNextExecutionOptions(
    val keepAliveFor: Duration,
    // todo: mtymes - change into enum with values: ONLY_NON_SUSPENDED, ONLY_SUSPENDED, SUSPENDED_AND_NON_SUSPENDED
    val fetchSuspendedTasksAsWell: Boolean = false,
    val newTTL: Duration? = null
) {
    init {
        expectPositiveDuration("keepAliveFor", keepAliveFor)
        expectNullOrPositiveDuration("newTTL", newTTL)
    }
}

data class MarkAsSucceededOptions(
    val newTTL: Duration? = null
) {
    init {
        expectNullOrNonNegativeDuration("newTTL", newTTL)
    }

    companion object {
        val DEFAULT = MarkAsSucceededOptions()
    }
}

data class MarkAsFailedButCanRetryOptions(
    val retryDelay: Duration? = null,
    val newTTL: Duration? = null
) {
    init {
        expectNullOrNonNegativeDuration("retryDelay", retryDelay)
        expectNullOrPositiveDuration("newTTL", newTTL)
    }

    companion object {
        val DEFAULT = MarkAsFailedButCanRetryOptions()
    }
}

data class MarkAsFailedButCanNOTRetryOptions(
    val newTTL: Duration? = null
) {
    init {
        expectNullOrNonNegativeDuration("newTTL", newTTL)
    }

    companion object {
        val DEFAULT = MarkAsFailedButCanNOTRetryOptions()
    }
}

data class MarkTaskAsCancelledOptions(
    val newTTL: Duration? = null
) {
    init {
        expectNullOrNonNegativeDuration("newTTL", newTTL)
    }

    companion object {
        val DEFAULT = MarkTaskAsCancelledOptions()
    }
}

data class MarkTasksAsCancelledOptions(
    val newTTL: Duration? = null
) {
    init {
        expectNullOrNonNegativeDuration("newTTL", newTTL)
    }

    companion object {
        val DEFAULT = MarkTasksAsCancelledOptions()
    }
}

data class MarkAsCancelledOptions(
    val newTTL: Duration? = null
) {
    init {
        expectNullOrNonNegativeDuration("newTTL", newTTL)
    }

    companion object {
        val DEFAULT = MarkAsCancelledOptions()
    }
}

data class MarkTaskAsPausedOptions(
    val newTTL: Duration? = null
) {
    init {
        expectNullOrPositiveDuration("newTTL", newTTL)
    }

    companion object {
        val DEFAULT = MarkTaskAsPausedOptions()
    }
}

data class MarkTasksAsPausedOptions(
    val newTTL: Duration? = null
) {
    init {
        expectNullOrPositiveDuration("newTTL", newTTL)
    }

    companion object {
        val DEFAULT = MarkTasksAsPausedOptions()
    }
}

data class MarkTaskAsUnPausedOptions(
    val newTTL: Duration? = null
) {
    init {
        expectNullOrPositiveDuration("newTTL", newTTL)
    }

    companion object {
        val DEFAULT = MarkTaskAsUnPausedOptions()
    }
}

data class MarkTasksAsUnPausedOptions(
    val newTTL: Duration? = null
) {
    init {
        expectNullOrPositiveDuration("newTTL", newTTL)
    }

    companion object {
        val DEFAULT = MarkTasksAsUnPausedOptions()
    }
}

data class MarkAsSuspendedOptions(
    val suspendFor: Duration,
    val newTTL: Duration? = null
) {
    init {
        expectNonNegativeDuration("suspendFor", suspendFor)
        expectNullOrPositiveDuration("newTTL", newTTL)
    }
}

data class MarkDeadExecutionsAsTimedOutOptions(
    val retryDelay: Duration? = null,
    val newTTL: Duration? = null
) {
    init {
        expectNullOrNonNegativeDuration("retryDelay", retryDelay)
        expectNullOrNonNegativeDuration("newTTL", newTTL)
    }

    companion object {
        val DEFAULT = MarkDeadExecutionsAsTimedOutOptions()
    }
}

data class RegisterHeartBeatOptions(
    val keepAliveFor: Duration,
    val affectsUpdatedAtField: Boolean = false,
    val newTTL: Duration? = null
) {
    init {
        expectPositiveDuration("keepAliveFor", keepAliveFor)
        expectNullOrPositiveDuration("newTTL", newTTL)
    }
}

data class UpdateTaskDataOptions(
    val newTTL: Duration? = null
) {
    init {
        expectNullOrNonNegativeDuration("newTTL", newTTL)
    }

    companion object {
        val DEFAULT = UpdateTaskDataOptions()
    }
}

data class UpdateExecutionDataOptions(
    val mustBeLastExecution: Boolean = true,
    val newTTL: Duration? = null
) {
    init {
        expectNullOrNonNegativeDuration("newTTL", newTTL)
    }
}
