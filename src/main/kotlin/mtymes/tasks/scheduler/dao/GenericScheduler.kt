package mtymes.tasks.scheduler.dao

import com.mongodb.client.MongoCollection
import mtymes.tasks.common.domain.WorkerId
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.UNIVERSAL_SCHEDULER
import mtymes.tasks.scheduler.domain.*
import org.bson.Document


data class SchedulerDefaults(

    val submitTaskOptions: SubmitTaskOptions? = null,

    val fetchNextExecutionOptions: FetchNextExecutionOptions? = null,

    val markAsSucceededOptions: MarkAsSucceededOptions? = null,

    val markAsFailedButCanRetryOptions: MarkAsFailedButCanRetryOptions? = null,

    val markAsFailedButCanNOTRetryOptions: MarkAsFailedButCanNOTRetryOptions? = null,

    val markTaskAsCancelledOptions: MarkTaskAsCancelledOptions? = null,

    val markTasksAsCancelledOptions: MarkTasksAsCancelledOptions? = null,

    val markAsCancelledOptions: MarkAsCancelledOptions? = null,

    val markTaskAsPausedOptions: MarkTaskAsPausedOptions? = null,

    val markTasksAsPausedOptions: MarkTasksAsPausedOptions? = null,

    val markTaskAsUnPausedOptions: MarkTaskAsUnPausedOptions? = null,

    val markTasksAsUnPausedOptions: MarkTasksAsUnPausedOptions? = null,

    val markAsSuspendedOptions: MarkAsSuspendedOptions? = null,

    val markDeadExecutionsAsTimedOutOptions: MarkDeadExecutionsAsTimedOutOptions? = null,

    val registerHeartBeatOptions: RegisterHeartBeatOptions? = null,

    val updateTaskDataOptions: UpdateTaskDataOptions? = null,

    val updateExecutionDataOptions: UpdateExecutionDataOptions? = null
)

// todo: mtymes - merge no options methods
// todo: mtymes - replace Document? return type with domain object
class GenericScheduler(
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
        options: MarkAsSucceededOptions,
        additionalTaskData: Document? = null,
        additionalExecutionData: Document? = null
    ): Document? {
        return scheduler.markAsSucceeded(
            coll = collection,
            executionId = executionId,
            options = options,
            additionalTaskData = additionalTaskData,
            additionalExecutionData = additionalExecutionData
        )
    }

    fun markAsSucceeded(
        executionId: ExecutionId,
        additionalTaskData: Document? = null,
        additionalExecutionData: Document? = null
    ): Document? {
        return markAsSucceeded(
            executionId = executionId,
            options = defaults.markAsSucceededOptions ?: MarkAsSucceededOptions.DEFAULT,
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
            options = defaults.markAsFailedButCanRetryOptions ?: MarkAsFailedButCanRetryOptions.DEFAULT,
            additionalTaskData = additionalTaskData,
            additionalExecutionData = additionalExecutionData
        )
    }

    fun markAsFailedButCanNOTRetry(
        executionId: ExecutionId,
        options: MarkAsFailedButCanNOTRetryOptions,
        additionalTaskData: Document? = null,
        additionalExecutionData: Document? = null
    ): Document? {
        return scheduler.markAsFailedButCanNOTRetry(
            coll = collection,
            executionId = executionId,
            options = options,
            additionalTaskData = additionalTaskData,
            additionalExecutionData = additionalExecutionData
        )
    }

    fun markAsFailedButCanNOTRetry(
        executionId: ExecutionId,
        additionalTaskData: Document? = null,
        additionalExecutionData: Document? = null
    ): Document? {
        return markAsFailedButCanNOTRetry(
            executionId = executionId,
            options = defaults.markAsFailedButCanNOTRetryOptions ?: MarkAsFailedButCanNOTRetryOptions.DEFAULT,
            additionalTaskData = additionalTaskData,
            additionalExecutionData = additionalExecutionData
        )
    }

    fun markTaskAsCancelled(
        taskId: TaskId,
        options: MarkTaskAsCancelledOptions,
        additionalTaskData: Document? = null
    ): Document? {
        return scheduler.markTaskAsCancelled(
            coll = collection,
            taskId = taskId,
            options = options,
            additionalTaskData = additionalTaskData
        )
    }

    fun markTaskAsCancelled(
        taskId: TaskId,
        additionalTaskData: Document? = null
    ): Document? {
        return markTaskAsCancelled(
            taskId = taskId,
            options = defaults.markTaskAsCancelledOptions ?: MarkTaskAsCancelledOptions.DEFAULT,
            additionalTaskData = additionalTaskData
        )
    }

    // todo: mtymes - add sample for this one
    fun markTasksAsCancelled(
        options: MarkTasksAsCancelledOptions,
        customConstraints: Document,
        additionalTaskData: Document? = null

    ): Long {
        return scheduler.markTasksAsCancelled(
            coll = collection,
            options = options,
            customConstraints = customConstraints,
            additionalTaskData = additionalTaskData
        )
    }

    // todo: mtymes - add sample for this one
    fun markTasksAsCancelled(
        customConstraints: Document,
        additionalTaskData: Document? = null

    ): Long {
        return markTasksAsCancelled(
            options = defaults.markTasksAsCancelledOptions ?: MarkTasksAsCancelledOptions.DEFAULT,
            customConstraints = customConstraints,
            additionalTaskData = additionalTaskData
        )
    }

    fun markAsCancelled(
        executionId: ExecutionId,
        options: MarkAsCancelledOptions,
        additionalTaskData: Document? = null,
        additionalExecutionData: Document? = null
    ): Document? {
        return scheduler.markAsCancelled(
            coll = collection,
            options = options,
            executionId = executionId,
            additionalTaskData = additionalTaskData,
            additionalExecutionData = additionalExecutionData
        )
    }

    fun markAsCancelled(
        executionId: ExecutionId,
        additionalTaskData: Document? = null,
        additionalExecutionData: Document? = null
    ): Document? {
        return markAsCancelled(
            options = defaults.markAsCancelledOptions ?: MarkAsCancelledOptions.DEFAULT,
            executionId = executionId,
            additionalTaskData = additionalTaskData,
            additionalExecutionData = additionalExecutionData
        )
    }

    // todo: mtymes - write a sample for this
    fun markTaskAsPaused(
        taskId: TaskId,
        options: MarkTaskAsPausedOptions,
        additionalTaskData: Document? = null
    ): Document? {
        return scheduler.markTaskAsPaused(
            coll = collection,
            taskId = taskId,
            options = options,
            additionalTaskData = additionalTaskData
        )
    }

    // todo: mtymes - write a sample for this
    fun markTaskAsPaused(
        taskId: TaskId,
        additionalTaskData: Document? = null
    ): Document? {
        return scheduler.markTaskAsPaused(
            coll = collection,
            taskId = taskId,
            options = defaults.markTaskAsPausedOptions ?: MarkTaskAsPausedOptions.DEFAULT,
            additionalTaskData = additionalTaskData
        )
    }

    // todo: mtymes - add sample for this one
    fun markTasksAsPaused(
        options: MarkTasksAsPausedOptions,
        customConstraints: Document,
        additionalTaskData: Document? = null

    ): Long {
        return scheduler.markTasksAsPaused(
            coll = collection,
            options = options,
            customConstraints = customConstraints,
            additionalTaskData = additionalTaskData
        )
    }

    // todo: mtymes - add sample for this one
    fun markTasksAsPaused(
        customConstraints: Document,
        additionalTaskData: Document? = null

    ): Long {
        return scheduler.markTasksAsPaused(
            coll = collection,
            options = defaults.markTasksAsPausedOptions ?: MarkTasksAsPausedOptions.DEFAULT,
            customConstraints = customConstraints,
            additionalTaskData = additionalTaskData
        )
    }

    // todo: mtymes - write a sample for this
    fun markTaskAsUnPaused(
        taskId: TaskId,
        options: MarkTaskAsUnPausedOptions,
        additionalTaskData: Document? = null
    ): Document? {
        return scheduler.markTaskAsUnPaused(
            coll = collection,
            taskId = taskId,
            options = options,
            additionalTaskData = additionalTaskData
        )
    }

    // todo: mtymes - write a sample for this
    fun markTaskAsUnPaused(
        taskId: TaskId,
        additionalTaskData: Document? = null
    ): Document? {
        return markTaskAsUnPaused(
            taskId = taskId,
            options = defaults.markTaskAsUnPausedOptions ?: MarkTaskAsUnPausedOptions.DEFAULT,
            additionalTaskData = additionalTaskData
        )
    }

    // todo: mtymes - add sample for this one
    fun markTasksAsUnPaused(
        options: MarkTasksAsUnPausedOptions,
        customConstraints: Document,
        additionalTaskData: Document? = null

    ): Long {
        return scheduler.markTasksAsUnPaused(
            coll = collection,
            options = options,
            customConstraints = customConstraints,
            additionalTaskData = additionalTaskData
        )
    }

    // todo: mtymes - add sample for this one
    fun markTasksAsUnPaused(
        customConstraints: Document,
        additionalTaskData: Document? = null

    ): Long {
        return markTasksAsUnPaused(
            options = defaults.markTasksAsUnPausedOptions ?: MarkTasksAsUnPausedOptions.DEFAULT,
            customConstraints = customConstraints,
            additionalTaskData = additionalTaskData
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
            options = defaults.markDeadExecutionsAsTimedOutOptions ?: MarkDeadExecutionsAsTimedOutOptions.DEFAULT,
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
        options: UpdateTaskDataOptions,
        additionalTaskData: Document
    ): Document? {
        return scheduler.updateTaskData(
            coll = collection,
            taskId = taskId,
            options = options,
            additionalTaskData = additionalTaskData
        )
    }

    fun updateTaskData(
        taskId: TaskId,
        additionalTaskData: Document
    ): Document? {
        return updateTaskData(
            taskId = taskId,
            options = defaults.updateTaskDataOptions ?: UpdateTaskDataOptions.DEFAULT,
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

    fun getTaskSummary(
        taskId: TaskId
    ): TaskSummary? {
        return scheduler.getTaskSummary(
            coll = collection,
            taskId = taskId
        )
    }

    private fun <T> defaultOptions(fieldPath: String, value: T?): T {
        return (value ?: throw IllegalStateException("'${fieldPath}' is NOT DEFINED"))
    }
}