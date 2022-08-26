package mtymes.tasks.scheduler.dao

import com.mongodb.client.MongoCollection
import mtymes.tasks.common.mongo.DocBuilder.Companion.emptyDoc
import mtymes.tasks.common.time.Durations.ZERO_SECONDS
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.UNIVERSAL_SCHEDULER
import mtymes.tasks.scheduler.domain.*
import mtymes.tasks.scheduler.domain.TaskId.Companion.uniqueTaskId
import org.bson.Document

// todo: mtymes - replace emptyDoc/default values with nullable value
// todo: mtymes - rename to DefaultOptions
data class SchedulerDefaults(

    // PARTIAL OPTIONS SECTION

    @Deprecated("replace with non partial options")
    val partialSubmitTaskOptions: PartialSubmitTaskOptions? = null,

    // DEFAULT OPTIONS SECTION

    val submitTaskOptions: SubmitTaskOptions? = null,

    val fetchNextExecutionOptions: FetchNextExecutionOptions? = null,

    val markAsFailedButCanRetryOptions: MarkAsFailedButCanRetryOptions? = null,

    val markAsSuspendedOptions: MarkAsSuspendedOptions? = null,

    val markDeadExecutionsAsTimedOutOptions: MarkDeadExecutionsAsTimedOutOptions? = null,

    val registerHeartBeatOptions: RegisterHeartBeatOptions? = null
)

// todo: mtymes - replace Document? return type with domain object
class GenericTaskScheduler(
    val collection: MongoCollection<Document>,
    val defaults: SchedulerDefaults,
    val scheduler: UniversalScheduler = UNIVERSAL_SCHEDULER
) {
    @Deprecated("replace with non partial options")
    fun submitTask(
        customData: Document,
        options: PartialSubmitTaskOptions? = null
    ): TaskId {
        val usedOptions: PartialSubmitTaskOptions = getOptionsToUse(
            "options", options,
            "this.defaults.partialSubmitTaskOptions", defaults.partialSubmitTaskOptions
        )

        return scheduler.submitTask(
            coll = collection,
            data = customData,
            options = SubmitTaskOptions(
                taskIdGenerator = usedOptions.taskIdGenerator ?: { uniqueTaskId() },
                maxAttemptsCount = usedOptions.maxAttemptsCount,
                ttl = usedOptions.ttl!!,
                delayStartBy = usedOptions.delayStartBy ?: ZERO_SECONDS
            )
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
        additionalTaskData: Document = emptyDoc(),
        additionalExecutionData: Document = emptyDoc()
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
        additionalTaskData: Document = emptyDoc(),
        additionalExecutionData: Document = emptyDoc()
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
        additionalTaskData: Document = emptyDoc(),
        additionalExecutionData: Document = emptyDoc()
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
        additionalTaskData: Document = emptyDoc(),
        additionalExecutionData: Document = emptyDoc()
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
        additionalTaskData: Document = emptyDoc()
    ): Document? {
        return scheduler.markTaskAsCancelled(
            coll = collection,
            taskId = taskId,
            additionalTaskData = additionalTaskData
        )
    }

    fun markExecutionAsCancelled(
        executionId: ExecutionId,
        additionalTaskData: Document = emptyDoc(),
        additionalExecutionData: Document = emptyDoc()
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
        additionalTaskData: Document = emptyDoc(),
        additionalExecutionData: Document = emptyDoc()
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
        additionalTaskData: Document = emptyDoc(),
        additionalExecutionData: Document = emptyDoc()
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
        additionalTaskData: Document = emptyDoc(),
        additionalExecutionData: Document = emptyDoc()
    ) {
        scheduler.markDeadExecutionsAsTimedOut(
            coll = collection,
            options = options,
            additionalTaskData = additionalTaskData,
            additionalExecutionData = additionalExecutionData
        )
    }

    fun markDeadExecutionsAsTimedOut(
        additionalTaskData: Document = emptyDoc(),
        additionalExecutionData: Document = emptyDoc()
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
        additionalTaskData: Document = emptyDoc(),
        additionalExecutionData: Document = emptyDoc()
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
        additionalTaskData: Document = emptyDoc(),
        additionalExecutionData: Document = emptyDoc()
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
        additionalTaskData: Document = emptyDoc(),
        additionalExecutionData: Document
    ): Document? {
        return scheduler.updateExecutionData(
            coll = collection,
            executionId = executionId,
            options = options,
            additionalTaskData = additionalTaskData,
            additionalExecutionData = additionalExecutionData
        )
    }

    private fun <T> defaultOptions(fieldPath: String, value: T?): T {
        return (value ?: throw IllegalStateException("'${fieldPath}' is NOT DEFINED"))
    }


    @Deprecated("replace with non partial options")
    private fun <T : CanBePartiallyDefined> getOptionsToUse(
        optionsParameterName: String,
        options: T?,
        defaultOptionsFieldPath: String,
        defaultOptions: T?
    ): T {
        val usedOptions: T
        if (options != null) {
            usedOptions = options.also {
                it.checkIsValid("${optionsParameterName}.")
            }
        } else {
            if (defaultOptions != null) {
                usedOptions = defaultOptions.also {
                    it.checkIsValid("${defaultOptionsFieldPath}.")
                }
            } else {
                throw IllegalStateException("Neither '${optionsParameterName}' parameter nor '${defaultOptionsFieldPath}' field are defined")
            }
        }
        return usedOptions
    }
}