package mtymes.task.v02.scheduler.dao

import com.mongodb.client.MongoCollection
import mtymes.task.v02.common.mongo.DocBuilder.Companion.emptyDoc
import mtymes.task.v02.scheduler.dao.UniversalScheduler.Companion.UNIVERSAL_SCHEDULER
import mtymes.task.v02.scheduler.domain.*
import org.bson.Document
import java.time.Duration

data class SchedulerDefaults(
    // submit task settings
    val ttlDuration: Duration,
    val taskIdGenerator: () -> TaskId = { TaskId.uniqueTaskId() },
    val maxAttemptCount: Int = 1,
    val delayStartBy: Duration = Duration.ofSeconds(0),

    // fetch task settings
    val afterStartKeepAliveFor: Duration,
    val additionalConstraint: Document = emptyDoc(),
    val sortOrder: Document = emptyDoc(),
    val areTasksSuspendable: Boolean = false,

    // task failure
    val retryDelayDuration: Duration = Duration.ofSeconds(0),

    // remainder to process

    val suspendForDuration: Duration = Duration.ofSeconds(0),
)


// todo: mtymes - should workerId be mandatory
class GenericTaskScheduler(
    val collection: MongoCollection<Document>,
    val defaults: SchedulerDefaults,
    val scheduler: UniversalScheduler = UNIVERSAL_SCHEDULER
) {

    fun submitTask(
        customData: Document,
        taskId: TaskId = defaults.taskIdGenerator.invoke(),
        ttlDuration: Duration = defaults.ttlDuration,
        maxAttemptCount: Int = defaults.maxAttemptCount,
        delayStartBy: Duration = defaults.delayStartBy
    ): TaskId? {
        return scheduler.submitTask(
            coll = collection,
            taskId = taskId,
            config = TaskConfig(
                maxAttemptCount = maxAttemptCount
            ),
            data = customData,
            ttlDuration = ttlDuration,
            delayStartBy = delayStartBy
        )
    }

    fun fetchNextAvailableExecution(
        workerId: WorkerId,
        keepAliveFor: Duration = defaults.afterStartKeepAliveFor,
        additionalConstraint: Document = defaults.additionalConstraint,
        sortOrder: Document = defaults.sortOrder,
        areTasksSuspendable: Boolean = defaults.areTasksSuspendable,
    ): StartedExecutionSummary? {
        return scheduler.fetchNextAvailableExecution(
            coll = collection,
            keepAliveForDuration = keepAliveFor,
            areTheseTasksSuspendable = areTasksSuspendable,
            additionalConstraint = additionalConstraint,
            workerId = workerId,
            sortOrder = sortOrder
        )
    }

    fun markAsSucceeded(
        executionId: ExecutionId,
        additionalExecutionData: Document = emptyDoc()
    ) {
        scheduler.markAsSucceeded(
            coll = collection,
            executionId = executionId,
            additionalExecutionData = additionalExecutionData
        )
    }

    fun markAsFailedButCanRetry(
        executionId: ExecutionId,
        retryDelayDuration: Duration = defaults.retryDelayDuration,
        additionalExecutionData: Document = emptyDoc()
    ) {
        scheduler.markAsFailedButCanRetry(
            coll = collection,
            executionId = executionId,
            retryDelayDuration = retryDelayDuration,
            additionalExecutionData = additionalExecutionData
        )
    }

    fun markAsFailedButCanNOTRetry(
        executionId: ExecutionId,
        additionalExecutionData: Document = emptyDoc()
    ) {
        scheduler.markAsFailedButCanNOTRetry(
            coll = collection,
            executionId = executionId,
            additionalExecutionData = additionalExecutionData
        )
    }
}