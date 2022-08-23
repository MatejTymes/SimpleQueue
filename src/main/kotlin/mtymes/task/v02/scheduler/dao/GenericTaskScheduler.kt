package mtymes.task.v02.scheduler.dao

import com.mongodb.client.MongoCollection
import mtymes.task.v02.common.mongo.DocBuilder.Companion.doc
import mtymes.task.v02.common.mongo.DocBuilder.Companion.emptyDoc
import mtymes.task.v02.scheduler.dao.UniversalScheduler.Companion.CAN_BE_EXECUTED_AS_OF
import mtymes.task.v02.scheduler.dao.UniversalScheduler.Companion.UNIVERSAL_SCHEDULER
import mtymes.task.v02.scheduler.domain.*
import org.bson.Document
import java.time.Duration

// todo: mtymes - replace emptyDoc/default values with nullable value
data class SchedulerDefaults(
    // submit task settings
    val ttlDuration: Duration,
    val taskIdGenerator: () -> TaskId = { TaskId.uniqueTaskId() },
    val maxAttemptCount: Int = 1,
    val delayStartBy: Duration = Duration.ofSeconds(0),

    // fetch task settings
    val afterStartKeepAliveFor: Duration,
    val additionalConstraint: Document = emptyDoc(),
    val customSortOrder: Document = doc(CAN_BE_EXECUTED_AS_OF to 1),
    val areTasksSuspendable: Boolean = false,

    // task failure
    val retryDelay: Duration = Duration.ofSeconds(0),

    // remainder to process

    val suspendFor: Duration = Duration.ofSeconds(0),
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
        customSortOrder: Document = defaults.customSortOrder,
        areTasksSuspendable: Boolean = defaults.areTasksSuspendable,
    ): StartedExecutionSummary? {
        return scheduler.fetchNextAvailableExecution(
            coll = collection,
            keepAliveFor = keepAliveFor,
            areTheseTasksSuspendable = areTasksSuspendable,
            additionalConstraint = additionalConstraint,
            workerId = workerId,
            sortOrder = customSortOrder
        )
    }

    fun markAsSucceeded(
        executionId: ExecutionId,
        additionalTaskData: Document = emptyDoc(),
        additionalExecutionData: Document = emptyDoc()
    ) {
        scheduler.markAsSucceeded(
            coll = collection,
            executionId = executionId,
            additionalTaskData = additionalTaskData,
            additionalExecutionData = additionalExecutionData
        )
    }

    fun markAsFailedButCanRetry(
        executionId: ExecutionId,
        retryDelay: Duration = defaults.retryDelay,
        additionalTaskData: Document = emptyDoc(),
        additionalExecutionData: Document = emptyDoc()
    ) {
        scheduler.markAsFailedButCanRetry(
            coll = collection,
            executionId = executionId,
            retryDelay = retryDelay,
            additionalTaskData = additionalTaskData,
            additionalExecutionData = additionalExecutionData
        )
    }

    fun markAsFailedButCanNOTRetry(
        executionId: ExecutionId,
        additionalTaskData: Document = emptyDoc(),
        additionalExecutionData: Document = emptyDoc()
    ) {
        scheduler.markAsFailedButCanNOTRetry(
            coll = collection,
            executionId = executionId,
            additionalTaskData = additionalTaskData,
            additionalExecutionData = additionalExecutionData
        )
    }

    fun markTaskAsCancelled(
        taskId: TaskId,
        additionalTaskData: Document = emptyDoc()
    ) {
        scheduler.markTaskAsCancelled(
            coll = collection,
            taskId = taskId,
            additionalTaskData = additionalTaskData
        )
    }

    fun markExecutionAsCancelled(
        executionId: ExecutionId,
        additionalExecutionData: Document = emptyDoc()
    ) {
        scheduler.markAsCancelled(
            coll = collection,
            executionId = executionId,
            additionalExecutionData = additionalExecutionData
        )
    }
}