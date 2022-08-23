package mtymes.task.v02.samples.sample04

import com.mongodb.client.MongoCollection
import mtymes.task.v02.common.mongo.DocBuilder
import mtymes.task.v02.common.mongo.DocBuilder.Companion.doc
import mtymes.task.v02.scheduler.dao.GenericTaskScheduler
import mtymes.task.v02.scheduler.dao.SchedulerDefaults
import mtymes.task.v02.scheduler.domain.ExecutionId
import mtymes.task.v02.scheduler.domain.TaskId
import mtymes.task.v02.scheduler.domain.WorkerId
import mtymes.task.v02.test.mongo.emptyLocalCollection
import mtymes.task.v02.test.task.TaskViewer
import org.bson.Document
import java.time.Duration


data class TaskToProcess(
    val taskId: TaskId,
    val executionId: ExecutionId,
    val request: String
)


class CancellationSupportingTaskDao(
    tasksCollection: MongoCollection<Document>
) {
    val scheduler = GenericTaskScheduler(
        collection = tasksCollection,
        defaults = SchedulerDefaults(
            maxAttemptCount = 3,
            ttlDuration = Duration.ofDays(7),
            afterStartKeepAliveFor = Duration.ofMinutes(5)
        )
    )

    companion object {
        const val REQUEST = "request"
    }

    fun submitTask(
        request: String
    ): TaskId? {
        return scheduler.submitTask(
            DocBuilder.doc(REQUEST to request)
        )
    }

    fun fetchNextTaskExecution(
        workerId: WorkerId
    ): TaskToProcess? {
        return scheduler.fetchNextAvailableExecution(workerId)
            ?.let { summary ->
                TaskToProcess(
                    taskId = summary.task.taskId,
                    executionId = summary.execution.executionId,
                    request = summary.task.data.getString(REQUEST)
                )
            }
    }

    fun markTaskAsCancelled(
        taskId: TaskId,
        cancellationReason: String
    ) {
        scheduler.markTaskAsCancelled(
            taskId = taskId,
            additionalTaskData = doc(
                "cancellationReason" to cancellationReason
            )
        )
    }

    fun markExecutionAsCancelled(
        executionId: ExecutionId,
        cancellationReason: String,
        incNumber: String
    ) {
        scheduler.markExecutionAsCancelled(
            executionId = executionId,
            additionalTaskData = doc(
                "cancellationReason" to cancellationReason
            ),
            additionalExecutionData = doc(
                "incNumber" to incNumber
            )
        )
    }
}



object CancelTask {

    @JvmStatic
    fun main(args: Array<String>) {
        val coll = emptyLocalCollection("sample04tasks")

        val dao = CancellationSupportingTaskDao(coll)

        val workerId = WorkerId("UnluckyInternDoingManualWork")


        val taskId = dao.submitTask("A")!!
        dao.markTaskAsCancelled(taskId, "clearing out the queue")


        val fetchedNext = dao.fetchNextTaskExecution(workerId)

        TaskViewer.displayTinyTasksSummary(
            coll,
            setOf("maxAttemptsCount", "attemptsLeft")
        )
        println("isThereAnythingAvailable = ${fetchedNext != null}")
    }
}



object CancelExecution {

    @JvmStatic
    fun main(args: Array<String>) {
        val coll = emptyLocalCollection("sample04tasks")

        val dao = CancellationSupportingTaskDao(coll)

        dao.submitTask("A")

        val workerId = WorkerId("UnluckyInternDoingManualWork")


        val executionId1 = dao.fetchNextTaskExecution(workerId)!!.executionId
        dao.markExecutionAsCancelled(executionId1, "subject no longer needed", "EO-66")


        val fetchedNext = dao.fetchNextTaskExecution(workerId)

        TaskViewer.displayTinyTasksSummary(
            coll,
            setOf("maxAttemptsCount", "attemptsLeft")
        )
        println("isThereAnythingAvailable = ${fetchedNext != null}")
    }
}
