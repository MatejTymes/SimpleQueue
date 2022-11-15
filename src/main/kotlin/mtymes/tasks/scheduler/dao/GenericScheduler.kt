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

    val markKillableExecutionsAsDeadOptions: MarkKillableExecutionsAsDeadOptions? = null,

    val registerHeartBeatOptions: RegisterHeartBeatOptions? = null,

    val updateTaskDataOptions: UpdateTaskDataOptions? = null,

    val updateExecutionDataOptions: UpdateExecutionDataOptions? = null
)

class GenericScheduler(
    val collection: MongoCollection<Document>,
    val defaults: SchedulerDefaults,
    val scheduler: UniversalScheduler = UNIVERSAL_SCHEDULER
) {

    fun findTask(
        taskId: TaskId
    ): Task? {
        return scheduler.findTask(
            coll = collection,
            taskId = taskId
        )
    }

    fun findExecution(
        executionId: ExecutionId
    ): ExecutionSummary? {
        return scheduler.findExecution(
            coll = collection,
            executionId = executionId
        )
    }

    fun findTasks(
        customConstraints: Document? = null,
        sortOrder: Document? = null
    ): Iterable<Task> {
        return scheduler.findTasks(
            coll = collection,
            customConstraints = customConstraints,
            sortOrder = sortOrder
        )
    }

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
    ): FetchedExecutionSummary? {
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
    ): FetchedExecutionSummary? {
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
    ): ExecutionSummary? {
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
    ): ExecutionSummary? {
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
    ): ExecutionSummary? {
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
    ): ExecutionSummary? {
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
    ): ExecutionSummary? {
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
    ): ExecutionSummary? {
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
    ): Task? {
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
    ): Task? {
        return markTaskAsCancelled(
            taskId = taskId,
            options = defaults.markTaskAsCancelledOptions ?: MarkTaskAsCancelledOptions.DEFAULT,
            additionalTaskData = additionalTaskData
        )
    }

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
    ): ExecutionSummary? {
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
    ): ExecutionSummary? {
        return markAsCancelled(
            options = defaults.markAsCancelledOptions ?: MarkAsCancelledOptions.DEFAULT,
            executionId = executionId,
            additionalTaskData = additionalTaskData,
            additionalExecutionData = additionalExecutionData
        )
    }

    fun markTaskAsPaused(
        taskId: TaskId,
        options: MarkTaskAsPausedOptions,
        additionalTaskData: Document? = null
    ): Task? {
        return scheduler.markTaskAsPaused(
            coll = collection,
            taskId = taskId,
            options = options,
            additionalTaskData = additionalTaskData
        )
    }

    fun markTaskAsPaused(
        taskId: TaskId,
        additionalTaskData: Document? = null
    ): Task? {
        return scheduler.markTaskAsPaused(
            coll = collection,
            taskId = taskId,
            options = defaults.markTaskAsPausedOptions ?: MarkTaskAsPausedOptions.DEFAULT,
            additionalTaskData = additionalTaskData
        )
    }

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

    fun markTaskAsUnPaused(
        taskId: TaskId,
        options: MarkTaskAsUnPausedOptions,
        additionalTaskData: Document? = null
    ): Task? {
        return scheduler.markTaskAsUnPaused(
            coll = collection,
            taskId = taskId,
            options = options,
            additionalTaskData = additionalTaskData
        )
    }

    fun markTaskAsUnPaused(
        taskId: TaskId,
        additionalTaskData: Document? = null
    ): Task? {
        return markTaskAsUnPaused(
            taskId = taskId,
            options = defaults.markTaskAsUnPausedOptions ?: MarkTaskAsUnPausedOptions.DEFAULT,
            additionalTaskData = additionalTaskData
        )
    }

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
    ): ExecutionSummary? {
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
    ): ExecutionSummary? {
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

    fun markKillableExecutionsAsDead(
        options: MarkKillableExecutionsAsDeadOptions,
        customConstraints: Document? = null,
        additionalTaskData: Document? = null,
        additionalExecutionData: Document? = null
    ): Int {
        return scheduler.markKillableExecutionsAsDead(
            coll = collection,
            options = options,
            customConstraints = customConstraints,
            additionalTaskData = additionalTaskData,
            additionalExecutionData = additionalExecutionData
        )
    }

    fun markKillableExecutionsAsDead(
        customConstraints: Document? = null,
        additionalTaskData: Document? = null,
        additionalExecutionData: Document? = null
    ): Int {
        return markKillableExecutionsAsDead(
            options = defaults.markKillableExecutionsAsDeadOptions ?: MarkKillableExecutionsAsDeadOptions.DEFAULT,
            customConstraints = customConstraints,
            additionalTaskData = additionalTaskData,
            additionalExecutionData = additionalExecutionData
        )
    }

    fun markKillableExecutionsAsDead(
        customConstraints: Document? = null,
        deadTaskUpdateProvider: (ExecutionSummary) -> DeadTaskUpdate
    ): Int {
        return scheduler.markKillableExecutionsAsDead(
            coll = collection,
            customConstraints = customConstraints,
            deadTaskUpdateProvider = deadTaskUpdateProvider
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

    fun registerHeartBeatAndProvideOutcome(
        executionId: ExecutionId,
        options: RegisterHeartBeatOptions,
        additionalTaskData: Document? = null,
        additionalExecutionData: Document? = null
    ): HeartBeatOutcome {
        return scheduler.registerHeartBeatAndProvideOutcome(
            coll = collection,
            executionId = executionId,
            options = options,
            additionalTaskData = additionalTaskData,
            additionalExecutionData = additionalExecutionData
        )
    }

    fun registerHeartBeatAndProvideOutcome(
        executionId: ExecutionId,
        additionalTaskData: Document? = null,
        additionalExecutionData: Document? = null
    ): HeartBeatOutcome {
        return registerHeartBeatAndProvideOutcome(
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
        customConstraints: Document? = null,
        additionalTaskData: Document
    ): Task? {
        return scheduler.updateTaskData(
            coll = collection,
            taskId = taskId,
            options = options,
            customConstraints = customConstraints,
            additionalTaskData = additionalTaskData
        )
    }

    fun updateTaskData(
        taskId: TaskId,
        customConstraints: Document? = null,
        additionalTaskData: Document
    ): Task? {
        return updateTaskData(
            taskId = taskId,
            options = defaults.updateTaskDataOptions ?: UpdateTaskDataOptions.DEFAULT,
            customConstraints = customConstraints,
            additionalTaskData = additionalTaskData
        )
    }

    fun updateExecutionData(
        executionId: ExecutionId,
        options: UpdateExecutionDataOptions,
        customConstraints: Document? = null,
        additionalExecutionData: Document,
        additionalTaskData: Document? = null
    ): ExecutionSummary? {
        return scheduler.updateExecutionData(
            coll = collection,
            executionId = executionId,
            options = options,
            customConstraints = customConstraints,
            additionalTaskData = additionalTaskData,
            additionalExecutionData = additionalExecutionData
        )
    }

    fun updateExecutionData(
        executionId: ExecutionId,
        customConstraints: Document? = null,
        additionalExecutionData: Document,
        additionalTaskData: Document? = null
    ): ExecutionSummary? {
        return updateExecutionData(
            executionId = executionId,
            options = defaults.updateExecutionDataOptions ?: UpdateExecutionDataOptions.DEFAULT,
            customConstraints = customConstraints,
            additionalTaskData = additionalTaskData,
            additionalExecutionData = additionalExecutionData
        )
    }

    private fun <T> defaultOptions(fieldPath: String, value: T?): T {
        return (value ?: throw IllegalStateException("NEITHER '${fieldPath}' value NOR 'options' parameter is DEFINED"))
    }
}