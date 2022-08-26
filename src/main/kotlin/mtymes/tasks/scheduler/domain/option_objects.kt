package mtymes.tasks.scheduler.domain

import mtymes.tasks.common.check.ValidityChecks.expectAtLeastOne
import mtymes.tasks.common.check.ValidityChecks.expectNonNegativeDuration
import mtymes.tasks.common.check.ValidityChecks.expectPositiveDuration
import mtymes.tasks.common.time.Durations.ZERO_SECONDS
import mtymes.tasks.scheduler.domain.TaskId.Companion.uniqueTaskId
import java.time.Duration

interface CanBePartiallyDefined {

    @Throws(IllegalArgumentException::class)
    fun checkIsValid(fieldPrefix: String)

    @Throws(IllegalArgumentException::class)
    fun checkIsValid() {
        checkIsValid("")
    }
}

// todo: mtymes - define the remaining Options

data class PartialSubmitTaskOptions(
    val taskIdGenerator: (() -> TaskId)? = null,
    val maxAttemptsCount: Int = 1,
    val ttl: Duration? = null,
    val delayStartBy: Duration? = null,
    // todo: mtymes - add startPaused: Boolean = false
) : CanBePartiallyDefined {

    override fun checkIsValid(fieldPrefix: String) {
        val errors = mutableListOf<String>()

        if (maxAttemptsCount < 1) {
            errors.add("'${fieldPrefix}maxAttemptCounts' must be at least 1 but is ${maxAttemptsCount} instead")
        }
        if (ttl == null) {
            errors.add("'${fieldPrefix}ttl' must be defined")
        } else if (ttl.isNegative || ttl.isZero) {
            errors.add("'${fieldPrefix}ttl' must be a positive value but was ${ttl} instead")
        }

        if (errors.isNotEmpty()) {
            throw IllegalArgumentException(errors.joinToString("; "))
        }
    }
}

// todo: mtymes - can't create these (as they have not fields): MarkAsSucceededOptions, MarkAsFailedButCanNOTRetryOptions, MarkAsCancelledOptions

data class SubmitTaskOptions(
    val taskIdGenerator: (() -> TaskId) = { uniqueTaskId() },
    val maxAttemptsCount: Int = 1,
    val ttl: Duration,
    val delayStartBy: Duration = ZERO_SECONDS
    // todo: mtymes - add startPaused: Boolean = false
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
    val fetchSuspendedTasksAsWell: Boolean = false
) {
    init {
        expectPositiveDuration("keepAliveFor", keepAliveFor)
    }
}

data class MarkAsFailedButCanRetryOptions(
    val retryDelay: Duration = ZERO_SECONDS
) {
    init {
        expectNonNegativeDuration("retryDelay", retryDelay)
    }
}

data class MarkAsSuspendedOptions(
    val suspendFor: Duration
) {
    init {
        expectNonNegativeDuration("suspendFor", suspendFor)
    }
}

data class MarkDeadExecutionsAsTimedOutOptions(
    val retryDelay: Duration = ZERO_SECONDS
) {
    init {
        expectNonNegativeDuration("retryDelay", retryDelay)
    }
}

data class UpdateExecutionDataOptions(
    val mustBeLastExecution: Boolean = true
)

data class RegisterHeartBeatOptions(
    val keepAliveFor: Duration
) {
    init {
        expectPositiveDuration("keepAliveFor", keepAliveFor)
    }
}
