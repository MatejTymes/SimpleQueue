package mtymes.task.v02.samples.sample04

import com.mongodb.client.MongoCollection
import mtymes.task.v02.common.mongo.DocBuilder.Companion.doc
import mtymes.task.v02.scheduler.dao.GenericTaskScheduler
import mtymes.task.v02.scheduler.dao.SchedulerDefaults
import mtymes.task.v02.scheduler.domain.ExecutionId
import mtymes.task.v02.scheduler.domain.TaskId
import mtymes.task.v02.scheduler.domain.WorkerId
import mtymes.task.v02.test.mongo.emptyLocalCollection
import mtymes.task.v02.test.task.TaskViewer
import org.bson.Document
import printTimedString
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

    fun submitTask(
        request: String
    ): TaskId? {
        val taskId = scheduler.submitTask(
            doc("request" to request)
        )

        printTimedString("submitted Task '${request}'")

        return taskId
    }

    fun fetchNextTaskExecution(
        workerId: WorkerId
    ): TaskToProcess? {
        val result = scheduler.fetchNextAvailableExecution(workerId)
            ?.let { summary ->
                TaskToProcess(
                    taskId = summary.task.taskId,
                    executionId = summary.execution.executionId,
                    request = summary.task.data.getString("request")
                )
            }

        if (result != null) {
            printTimedString("fetched Execution ${result.executionId}")
        } else {
            printTimedString("did NOT fetch any Execution")
        }

        return result
    }

    fun markTaskAsCancelled(
        taskId: TaskId,
        cancellationReason: String
    ) {
        val result = scheduler.markTaskAsCancelled(
            taskId = taskId,
            additionalTaskData = doc(
                "cancellationReason" to cancellationReason
            )
        )
        if (result != null) {
            printTimedString("marked Task ${taskId} as CANCELLED")
        } else {
            printTimedString("did NOT mark Task ${taskId} as CANCELLED")
        }
    }

    fun markExecutionAsCancelled(
        executionId: ExecutionId,
        cancellationReason: String,
        incNumber: String
    ) {
        val result = scheduler.markExecutionAsCancelled(
            executionId = executionId,
            additionalTaskData = doc(
                "cancellationReason" to cancellationReason
            ),
            additionalExecutionData = doc(
                "incNumber" to incNumber
            )
        )
        if (result != null) {
            printTimedString("marked Execution ${executionId} as CANCELLED")
        } else {
            printTimedString("did NOT mark Execution ${executionId} as CANCELLED")
        }
    }
}



object CancelTask {

    @JvmStatic
    fun main(args: Array<String>) {
        val workerId = WorkerId("UnluckyInternDoingManualWork")
        val coll = emptyLocalCollection("sample04tasks")
        val dao = CancellationSupportingTaskDao(coll)


        val taskId = dao.submitTask("A")!!
        dao.markTaskAsCancelled(taskId, "clearing out the queue")


        dao.fetchNextTaskExecution(workerId)

        TaskViewer.displayTinyTasksSummary(
            coll,
            setOf("maxAttemptsCount", "attemptsLeft")
        )
    }
}



object CancelExecution {

    @JvmStatic
    fun main(args: Array<String>) {
        val workerId = WorkerId("UnluckyInternDoingManualWork")
        val coll = emptyLocalCollection("sample04tasks")
        val dao = CancellationSupportingTaskDao(coll)

        dao.submitTask("A")


        val executionId1 = dao.fetchNextTaskExecution(workerId)!!.executionId
        dao.markExecutionAsCancelled(executionId1, "subject no longer needed", "EO-66")


        dao.fetchNextTaskExecution(workerId)

        TaskViewer.displayTinyTasksSummary(
            coll,
            setOf("maxAttemptsCount", "attemptsLeft")
        )
    }
}



object FailToCancelTaskInProgress {

    @JvmStatic
    fun main(args: Array<String>) {
        val workerId = WorkerId("UnluckyInternDoingManualWork")
        val coll = emptyLocalCollection("sample04tasks")
        val dao = CancellationSupportingTaskDao(coll)

        dao.submitTask("A")


        try {

            val taskToProcess = dao.fetchNextTaskExecution(workerId)!!
            dao.markTaskAsCancelled(taskToProcess.taskId, "clearing out the queue")

        } catch (e: Exception) {
            e.printStackTrace()
        }


        TaskViewer.displayTinyTasksSummary(
            coll,
            setOf("maxAttemptsCount", "attemptsLeft")
        )
    }
}
