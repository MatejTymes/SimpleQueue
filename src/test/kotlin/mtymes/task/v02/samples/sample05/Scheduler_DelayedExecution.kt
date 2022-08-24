package mtymes.task.v02.samples.sample05

import com.mongodb.client.MongoCollection
import mtymes.task.v02.common.mongo.DocBuilder.Companion.doc
import mtymes.task.v02.scheduler.dao.GenericTaskScheduler
import mtymes.task.v02.scheduler.dao.SchedulerDefaults
import mtymes.task.v02.scheduler.domain.ExecutionId
import mtymes.task.v02.scheduler.domain.WorkerId
import mtymes.task.v02.test.mongo.emptyLocalCollection
import mtymes.task.v02.test.task.TaskViewer
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
    val scheduler = GenericTaskScheduler(
        collection = tasksCollection,
        defaults = SchedulerDefaults(
            maxAttemptCount = 3,
            ttlDuration = Duration.ofDays(7),
            afterStartKeepAliveFor = Duration.ofMinutes(5),

            fetchSuspendedTasksAsWell = true
        )
    )

    fun submitTask(
        request: String,
        delayStartBy: Duration = Duration.ofSeconds(0)
    ) {
        scheduler.submitTask(
            customData = doc("request" to request),
            delayStartBy = delayStartBy
        )

        printTimedString("submitted Task '${request}'")
    }

    fun fetchNextTaskExecution(
        workerId: WorkerId,
        fetchSuspendedTasksAsWell: Boolean = scheduler.defaults.fetchSuspendedTasksAsWell
    ): TaskToProcess? {
        val result = scheduler.fetchNextAvailableExecution(
            workerId = workerId,
            fetchSuspendedTasksAsWell = fetchSuspendedTasksAsWell
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
            suspendFor = suspendFor
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
