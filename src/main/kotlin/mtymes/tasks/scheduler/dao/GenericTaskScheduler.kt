package mtymes.tasks.scheduler.dao

import com.mongodb.client.MongoCollection
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.UNIVERSAL_SCHEDULER
import mtymes.tasks.scheduler.domain.*
import org.bson.Document

// todo: mtymes - rename to DefaultOptions
data class SchedulerDefaults(

    val submitTaskOptions: SubmitTaskOptions? = null,

    val fetchNextExecutionOptions: FetchNextExecutionOptions? = null,

    val markAsFailedButCanRetryOptions: MarkAsFailedButCanRetryOptions? = null,

    val markAsSuspendedOptions: MarkAsSuspendedOptions? = null,

    val markDeadExecutionsAsTimedOutOptions: MarkDeadExecutionsAsTimedOutOptions? = null,

    val registerHeartBeatOptions: RegisterHeartBeatOptions? = null,

    val updateExecutionDataOptions: UpdateExecutionDataOptions? = null
)

// todo: mtymes - replace Document? return type with domain object
class GenericTaskScheduler(
    val collection: MongoCollection<Document>,
    val defaults: SchedulerDefaults,
    val scheduler: UniversalScheduler = UNIVERSAL_SCHEDULER
) {

    fun submitTask(
        customData: Document,
        options: SubmitTaskOptions
    ): TaskId {
        return scheduler.submitTask(
            coll = collection,
            data = customData,
            options = options
        )
    }

    fun submitTask(
        customData: Document
    ): TaskId {
        return submitTask(
            customData = customData,
            options = defaultOptions(
                "this.defaults.submitTaskOptions",
                defaults.submitTaskOptions
            )
        )
    }

    fun fetchNextAvailableExecution(
        workerId: WorkerId,
        options: FetchNextExecutionOptions,
        additionalConstraints: Document? = null,
        customSortOrder: Document? = null
    ): StartedExecutionSummary? {
        return scheduler.fetchNextAvailableExecution(
            coll = collection,
            workerId = workerId,
            options = options,
            additionalConstraints = additionalConstraints,
            sortOrder = customSortOrder
        )
    }

    fun fetchNextAvailableExecution(
        workerId: WorkerId,
        additionalConstraints: Document? = null,
        customSortOrder: Document? = null
    ): StartedExecutionSummary? {
        return fetchNextAvailableExecution(
            workerId = workerId,
            options = defaultOptions(
                "this.defaults.fetchNextExecutionOptions",
                defaults.fetchNextExecutionOptions
            ),
            additionalConstraints = additionalConstraints,
            customSortOrder = customSortOrder
        )
    }

    fun markAsSucceeded(
        executionId: ExecutionId,
        additionalTaskData: Document? = null,
        additionalExecutionData: Document? = null
    ): Document? {
        return scheduler.markAsSucceeded(
            coll = collection,
            executionId = executionId,
            additionalTaskData = additionalTaskData,
            additionalExecutionData = additionalExecutionData
        )
    }

    fun markAsFailedButCanRetry(
        executionId: ExecutionId,
        options: MarkAsFailedButCanRetryOptions,
        additionalTaskData: Document? = null,
        additionalExecutionData: Document? = null
    ): Document? {
        return scheduler.markAsFailedButCanRetry(
            coll = collection,
            executionId = executionId,
            options = options,
            additionalTaskData = additionalTaskData,
            additionalExecutionData = additionalExecutionData
        )
    }

    fun markAsFailedButCanRetry(
        executionId: ExecutionId,
        additionalTaskData: Document? = null,
        additionalExecutionData: Document? = null
    ): Document? {
        return markAsFailedButCanRetry(
            executionId = executionId,
            options = defaultOptions(
                "this.defaults.markAsFailedButCanRetryOptions",
                defaults.markAsFailedButCanRetryOptions
            ),
            additionalTaskData = additionalTaskData,
            additionalExecutionData = additionalExecutionData
        )
    }

    fun markAsFailedButCanNOTRetry(
        executionId: ExecutionId,
        additionalTaskData: Document? = null,
        additionalExecutionData: Document? = null
    ): Document? {
        return scheduler.markAsFailedButCanNOTRetry(
            coll = collection,
            executionId = executionId,
            additionalTaskData = additionalTaskData,
            additionalExecutionData = additionalExecutionData
        )
    }

    fun markTaskAsCancelled(
        taskId: TaskId,
        additionalTaskData: Document? = null
    ): Document? {
        return scheduler.markTaskAsCancelled(
            coll = collection,
            taskId = taskId,
            additionalTaskData = additionalTaskData
        )
    }

    fun markExecutionAsCancelled(
        executionId: ExecutionId,
        additionalTaskData: Document? = null,
        additionalExecutionData: Document? = null
    ): Document? {
        return scheduler.markAsCancelled(
            coll = collection,
            executionId = executionId,
            additionalTaskData = additionalTaskData,
            additionalExecutionData = additionalExecutionData
        )
    }

    fun markAsSuspended(
        executionId: ExecutionId,
        options: MarkAsSuspendedOptions,
        additionalTaskData: Document? = null,
        additionalExecutionData: Document? = null
    ): Document? {
        return scheduler.markAsSuspended(
            coll = collection,
            executionId = executionId,
            options = options,
            additionalTaskData = additionalTaskData,
            additionalExecutionData = additionalExecutionData
        )
    }

    fun markAsSuspended(
        executionId: ExecutionId,
        additionalTaskData: Document? = null,
        additionalExecutionData: Document? = null
    ): Document? {
        return markAsSuspended(
            executionId = executionId,
            options = defaultOptions(
                "this.defaults.markAsSuspendedOptions",
                defaults.markAsSuspendedOptions
            ),
            additionalTaskData = additionalTaskData,
            additionalExecutionData = additionalExecutionData
        )
    }

    fun markDeadExecutionsAsTimedOut(
        options: MarkDeadExecutionsAsTimedOutOptions,
        additionalTaskData: Document? = null,
        additionalExecutionData: Document? = null
    ) {
        scheduler.markDeadExecutionsAsTimedOut(
            coll = collection,
            options = options,
            additionalTaskData = additionalTaskData,
            additionalExecutionData = additionalExecutionData
        )
    }

    fun markDeadExecutionsAsTimedOut(
        additionalTaskData: Document? = null,
        additionalExecutionData: Document? = null
    ) {
        return markDeadExecutionsAsTimedOut(
            options = defaultOptions(
                "this.defaults.markDeadExecutionsAsTimedOutOptions",
                defaults.markDeadExecutionsAsTimedOutOptions
            ),
            additionalTaskData = additionalTaskData,
            additionalExecutionData = additionalExecutionData
        )
    }

    fun registerHeartBeat(
        executionId: ExecutionId,
        options: RegisterHeartBeatOptions,
        additionalTaskData: Document? = null,
        additionalExecutionData: Document? = null
    ): Boolean {
        return scheduler.registerHeartBeat(
            coll = collection,
            executionId = executionId,
            options = options,
            additionalTaskData = additionalTaskData,
            additionalExecutionData = additionalExecutionData
        )
    }

    fun registerHeartBeat(
        executionId: ExecutionId,
        additionalTaskData: Document? = null,
        additionalExecutionData: Document? = null
    ): Boolean {
        return registerHeartBeat(
            executionId = executionId,
            options = defaultOptions(
                "this.defaults.registerHeartBeatOptions",
                defaults.registerHeartBeatOptions
            ),
            additionalTaskData = additionalTaskData,
            additionalExecutionData = additionalExecutionData
        )
    }

    fun updateTaskData(
        taskId: TaskId,
        additionalTaskData: Document
    ): Document? {
        return scheduler.updateTaskData(
            coll = collection,
            taskId = taskId,
            additionalTaskData = additionalTaskData
        )
    }

    fun updateExecutionData(
        executionId: ExecutionId,
        options: UpdateExecutionDataOptions,
        additionalExecutionData: Document,
        additionalTaskData: Document? = null
    ): Document? {
        return scheduler.updateExecutionData(
            coll = collection,
            executionId = executionId,
            options = options,
            additionalTaskData = additionalTaskData,
            additionalExecutionData = additionalExecutionData
        )
    }

    fun updateExecutionData(
        executionId: ExecutionId,
        additionalExecutionData: Document,
        additionalTaskData: Document? = null
    ): Document? {
        return updateExecutionData(
            executionId = executionId,
            options = defaultOptions(
                "this.defaults.updateExecutionDataOptions",
                defaults.updateExecutionDataOptions
            ),
            additionalTaskData = additionalTaskData,
            additionalExecutionData = additionalExecutionData
        )
    }

    private fun <T> defaultOptions(fieldPath: String, value: T?): T {
        return (value ?: throw IllegalStateException("'${fieldPath}' is NOT DEFINED"))
    }
}