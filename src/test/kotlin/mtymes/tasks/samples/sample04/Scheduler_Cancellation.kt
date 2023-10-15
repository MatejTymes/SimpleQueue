package mtymes.tasks.samples.sample04

import com.mongodb.client.MongoCollection
import mtymes.tasks.common.domain.WorkerId
import mtymes.tasks.common.mongo.builder.WithBaseDocBuilder
import mtymes.tasks.common.time.Durations
import mtymes.tasks.scheduler.dao.GenericScheduler
import mtymes.tasks.scheduler.dao.SchedulerDefaults
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.EXECUTIONS_COUNT
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.EXECUTION_ATTEMPTS_LEFT
import mtymes.tasks.scheduler.domain.ExecutionId
import mtymes.tasks.scheduler.domain.PickNextExecutionOptions
import mtymes.tasks.scheduler.domain.SubmitTaskOptions
import mtymes.tasks.scheduler.domain.TaskId
import mtymes.tasks.test.mongo.emptyLocalCollection
import mtymes.tasks.test.task.TaskViewer.displayTinyTasksSummary
import org.bson.Document
import printTimedString


data class TaskToProcess(
    val taskId: TaskId,
    val executionId: ExecutionId,
    val request: String
)


class CancellationSupportingTaskDao(
    tasksCollection: MongoCollection<Document>
) : WithBaseDocBuilder {
    val scheduler = GenericScheduler(
        collection = tasksCollection,
        defaults = SchedulerDefaults(

            submitTaskOptions = SubmitTaskOptions(
                ttl = Durations.SEVEN_DAYS,
                maxAttemptsCount = 3
            ),

            pickNextExecutionOptions = PickNextExecutionOptions(
                keepAliveFor = Durations.FIVE_MINUTES
            )
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

    fun pickNextTaskExecution(
        workerId: WorkerId
    ): TaskToProcess? {
        val result = scheduler.pickNextAvailableExecution(workerId)
            ?.let { summary ->
                TaskToProcess(
                    taskId = summary.underlyingTask.taskId,
                    executionId = summary.pickedExecution.executionId,
                    request = summary.underlyingTask.data().getString("request")
                )
            }

        if (result != null) {
            printTimedString("picked Execution ${result.executionId}")
        } else {
            printTimedString("did NOT pick any Execution")
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
        val result = scheduler.markAsCancelled(
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


        dao.pickNextTaskExecution(workerId)

        displayTinyTasksSummary(coll, setOf(
            EXECUTION_ATTEMPTS_LEFT,
            EXECUTIONS_COUNT
        ))
    }
}


object CancelExecution {

    @JvmStatic
    fun main(args: Array<String>) {
        val workerId = WorkerId("UnluckyInternDoingManualWork")
        val coll = emptyLocalCollection("sample04tasks")
        val dao = CancellationSupportingTaskDao(coll)

        dao.submitTask("A")


        val executionId1 = dao.pickNextTaskExecution(workerId)!!.executionId
        dao.markExecutionAsCancelled(executionId1, "subject no longer needed", "EO-66")


        dao.pickNextTaskExecution(workerId)

        displayTinyTasksSummary(coll, setOf(
            EXECUTION_ATTEMPTS_LEFT,
            EXECUTIONS_COUNT
        ))
    }
}


object FailToCancelRunningTask {

    @JvmStatic
    fun main(args: Array<String>) {
        val workerId = WorkerId("UnluckyInternDoingManualWork")
        val coll = emptyLocalCollection("sample04tasks")
        val dao = CancellationSupportingTaskDao(coll)

        dao.submitTask("A")


        try {

            val taskToProcess = dao.pickNextTaskExecution(workerId)!!
            dao.markTaskAsCancelled(taskToProcess.taskId, "clearing out the queue")

        } catch (e: Exception) {
            printTimedString(e.message!!)
        }


        displayTinyTasksSummary(coll, setOf(
            EXECUTION_ATTEMPTS_LEFT,
            EXECUTIONS_COUNT
        ))
    }
}
