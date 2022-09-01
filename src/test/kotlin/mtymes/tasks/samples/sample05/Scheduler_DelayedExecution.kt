package mtymes.tasks.samples.sample05

import com.mongodb.client.MongoCollection
import mtymes.tasks.common.domain.WorkerId
import mtymes.tasks.common.mongo.DocBuilder.Companion.doc
import mtymes.tasks.common.time.Durations
import mtymes.tasks.scheduler.dao.GenericScheduler
import mtymes.tasks.scheduler.dao.SchedulerDefaults
import mtymes.tasks.scheduler.domain.ExecutionId
import mtymes.tasks.scheduler.domain.FetchNextExecutionOptions
import mtymes.tasks.scheduler.domain.MarkAsSuspendedOptions
import mtymes.tasks.scheduler.domain.SubmitTaskOptions
import mtymes.tasks.test.mongo.emptyLocalCollection
import mtymes.tasks.test.task.TaskViewer
import org.bson.Document
import printTimedString
import java.time.Duration


data class TaskToProcess(
    val executionId: ExecutionId,
    val request: String
)


class SuspendingTaskDao(
    tasksCollection: MongoCollection<Document>
) {
    val scheduler = GenericScheduler(
        collection = tasksCollection,
        defaults = SchedulerDefaults(

            submitTaskOptions = SubmitTaskOptions(
                ttl = Durations.SEVEN_DAYS,
                maxAttemptsCount = 3
            ),

            fetchNextExecutionOptions = FetchNextExecutionOptions(
                keepAliveFor = Durations.FIVE_MINUTES,
                fetchSuspendedTasksAsWell = true
            ),

            markAsSuspendedOptions = MarkAsSuspendedOptions(
                suspendFor = Durations.ZERO_SECONDS
            )
        )
    )

    fun submitTask(
        request: String,
        delayStartBy: Duration = Duration.ofSeconds(0)
    ) {
        scheduler.submitTask(
            customData = doc("request" to request),
            options = scheduler.defaults.submitTaskOptions!!.copy(
                delayStartBy = delayStartBy
            )
        )

        printTimedString("submitted Task '${request}'")
    }

    fun fetchNextTaskExecution(
        workerId: WorkerId,
        fetchSuspendedTasksAsWell: Boolean = scheduler.defaults.fetchNextExecutionOptions!!.fetchSuspendedTasksAsWell
    ): TaskToProcess? {
        val result = scheduler.fetchNextAvailableExecution(
            workerId = workerId,
            options = scheduler.defaults.fetchNextExecutionOptions!!.copy(
                fetchSuspendedTasksAsWell = fetchSuspendedTasksAsWell
            )
        )?.let { summary ->
            TaskToProcess(
                executionId = summary.execution.executionId,
                request = summary.task.data.getString("request")
            )
        }

        if (result != null) {
            printTimedString("fetched Execution ${result.executionId}")
        } else {
            printTimedString("did NOT fetch any Execution" + if(!fetchSuspendedTasksAsWell) " (fetching only NON-SUSPENDED tasks)" else "")
        }

        return result
    }

    fun markAsSuspended(
        executionId: ExecutionId,
        suspendFor: Duration
    ) {
        val result = scheduler.markAsSuspended(
            executionId = executionId,
            options = scheduler.defaults.markAsSuspendedOptions!!.copy(
                suspendFor = suspendFor
            )
        )
        if (result != null) {
            printTimedString("marked Execution ${executionId} as SUSPENDED for ${suspendFor}")
        } else {
            printTimedString("did NOT mark Execution ${executionId} as SUSPENDED")
        }
    }

    fun markAsSucceeded(
        executionId: ExecutionId,
        message: String
    ) {
        val result = scheduler.markAsSucceeded(
            executionId = executionId,
            additionalExecutionData = doc(
                "successMessage" to message
            )
        )
        if (result != null) {
            printTimedString("marked Execution ${executionId} as SUCCEEDED")
        } else {
            printTimedString("did NOT mark Execution ${executionId} as SUCCEEDED")
        }
    }
}


object DelayedStart {

    @JvmStatic
    fun main(args: Array<String>) {
        val workerId = WorkerId("UnluckyInternDoingManualWork")
        val coll = emptyLocalCollection("sample05tasks")
        val dao = SuspendingTaskDao(coll)


        dao.submitTask(
            request = "A",
            delayStartBy = Duration.ofSeconds(1)
        )

        TaskViewer.displayTinyTasksSummary(
            coll,
            setOf("maxAttemptsCount", "attemptsLeft", "canBeExecutedAsOf")
        )

        dao.fetchNextTaskExecution(workerId)


        Thread.sleep(1_100)


        dao.fetchNextTaskExecution(workerId)

        TaskViewer.displayTinyTasksSummary(
            coll,
            setOf("maxAttemptsCount", "attemptsLeft", "canBeExecutedAsOf")
        )
    }
}


object TaskSuspension {

    @JvmStatic
    fun main(args: Array<String>) {
        val workerId = WorkerId("UnluckyInternDoingManualWork")
        val coll = emptyLocalCollection("sample05tasks")
        val dao = SuspendingTaskDao(coll)

        dao.submitTask(request = "A")


        val executionId = dao.fetchNextTaskExecution(workerId)!!.executionId
        dao.markAsSuspended(
            executionId = executionId,
            suspendFor = Duration.ofSeconds(0)
        )

        TaskViewer.displayTinyTasksSummary(
            coll,
            setOf("maxAttemptsCount", "attemptsLeft", "canBeExecutedAsOf", "executions.suspensionCount")
        )

        // fetch non-suspendable only
        dao.fetchNextTaskExecution(
            workerId = workerId,
            fetchSuspendedTasksAsWell = false
        )

        // fetch suspendable as well
        val anotherExecutionId = dao.fetchNextTaskExecution(
            workerId = workerId,
            fetchSuspendedTasksAsWell = true
        )!!.executionId

        TaskViewer.displayTinyTasksSummary(
            coll,
            setOf("maxAttemptsCount", "attemptsLeft", "canBeExecutedAsOf", "executions.suspensionCount")
        )

        dao.markAsSuspended(
            executionId = anotherExecutionId,
            suspendFor = Duration.ofSeconds(1)
        )

        TaskViewer.displayTinyTasksSummary(
            coll,
            setOf("maxAttemptsCount", "attemptsLeft", "canBeExecutedAsOf", "executions.suspensionCount")
        )
        dao.fetchNextTaskExecution(
            workerId = workerId
        )


        Thread.sleep(1_100)


        dao.fetchNextTaskExecution(
            workerId = workerId
        )
    }
}

