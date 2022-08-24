package mtymes.task.v02.samples.sample03

import com.mongodb.client.MongoCollection
import mtymes.task.v02.common.mongo.DocBuilder.Companion.doc
import mtymes.task.v02.scheduler.dao.GenericTaskScheduler
import mtymes.task.v02.scheduler.dao.SchedulerDefaults
import mtymes.task.v02.scheduler.domain.ExecutionId
import mtymes.task.v02.scheduler.domain.WorkerId
import mtymes.task.v02.test.mongo.emptyLocalCollection
import mtymes.task.v02.test.task.TaskViewer.displayTinyTasksSummary
import mtymes.task.v02.worker.Worker
import mtymes.task.v02.worker.sweatshop.HumbleSweatShop
import org.bson.Document
import printTimedString
import java.time.Duration


data class TaskToProcess(
    val executionId: ExecutionId,
    val request: String
)



class FailureSupportingTaskDao(
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
    ) {
        scheduler.submitTask(
            doc(REQUEST to request)
        )
        printTimedString("submitted Task '${request}'")
    }

    fun fetchNextTaskExecution(
        workerId: WorkerId
    ): TaskToProcess? {
        val result = scheduler.fetchNextAvailableExecution(workerId)
            ?.let { summary ->
                TaskToProcess(
                    executionId = summary.execution.executionId,
                    request = summary.task.data.getString(REQUEST)
                )
            }

        if (result != null) {
            printTimedString("fetched Execution ${result.executionId}")
        } else {
            printTimedString("did NOT fetch any Execution")
        }

        return result
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

    fun markAsFailed(
        executionId: ExecutionId,
        e: Exception
    ) {
        val result = scheduler.markAsFailedButCanRetry(
            executionId = executionId,
            additionalExecutionData = doc(
                "failureMessage" to e.message
            )
        )
        if (result != null) {
            printTimedString("marked Execution ${executionId} as FAILED")
        } else {
            printTimedString("did NOT mark Execution ${executionId} as FAILED")
        }
    }

    fun markAsFailedAndCanNOTRetry(
        executionId: ExecutionId,
        e: Exception
    ) {
        val result = scheduler.markAsFailedButCanNOTRetry(
            executionId = executionId,
            additionalExecutionData = doc(
                "failureMessage" to e.message
            )
        )
        if (result != null) {
            printTimedString("marked Execution ${executionId} as FAILED WITHOUT RETRY")
        } else {
            printTimedString("did NOT mark Execution ${executionId} as FAILED WITHOUT RETRY")
        }
    }

    fun markAsFailed(
        executionId: ExecutionId,
        e: Exception,
        retryDelay: Duration
    ) {
        val result = scheduler.markAsFailedButCanRetry(
            executionId = executionId,
            additionalExecutionData = doc(
                "failureMessage" to e.message
            ),
            retryDelay = retryDelay
        )
        if (result != null) {
            printTimedString("marked Execution ${executionId} as FAILED with retry delay of ${retryDelay}")
        } else {
            printTimedString("did NOT mark Execution ${executionId} as FAILED")
        }
    }
}



class BrokenWorker(
    val dao: FailureSupportingTaskDao
) : Worker<TaskToProcess> {

    override fun fetchNextTaskToProcess(workerId: WorkerId): TaskToProcess? {
        return dao.fetchNextTaskExecution(workerId)
    }

    override fun executeTask(task: TaskToProcess, workerId: WorkerId) {
        // some complex logic
        Thread.sleep(1_000)

        throw IllegalStateException("Oh no! We failed!!!")
    }

    override fun handleExecutionFailure(task: TaskToProcess, workerId: WorkerId, exception: Exception) {
        dao.markAsFailed(task.executionId, exception)
    }

    override fun taskToLoggableString(task: TaskToProcess, workerId: WorkerId): String {
        return task.request
    }
}



object WorkerFailing {

    @JvmStatic
    fun main(args: Array<String>) {
        val coll = emptyLocalCollection("sample03tasks")
        val dao = FailureSupportingTaskDao(coll)

        dao.submitTask("A")

        HumbleSweatShop().use { sweatShop ->

            val worker = BrokenWorker(dao)

            sweatShop.addAndStartWorker(worker)

            Thread.sleep(4_500)
        }

        displayTinyTasksSummary(
            coll,
            setOf("maxAttemptsCount", "attemptsLeft", "executions.startedAt", "executions.finishedAt")
        )
    }
}



object FailThenSucceed {

    @JvmStatic
    fun main(args: Array<String>) {
        val workerId = WorkerId("UnluckyInternDoingManualWork")
        val coll = emptyLocalCollection("sample03tasks")
        val dao = FailureSupportingTaskDao(coll)

        dao.submitTask("A")


        val executionId1 = dao.fetchNextTaskExecution(workerId)!!.executionId
        dao.markAsFailed(executionId1, IllegalStateException("It should have worked"))


        displayTinyTasksSummary(
            coll,
            setOf("maxAttemptsCount", "attemptsLeft")
        )


        val executionId2 = dao.fetchNextTaskExecution(workerId)!!.executionId
        dao.markAsSucceeded(executionId2, "So glad it's over. I'm not doing this again")


        displayTinyTasksSummary(
            coll,
            setOf("maxAttemptsCount", "attemptsLeft")
        )


        dao.fetchNextTaskExecution(workerId)
    }
}



object UnrecoverableFailure {

    @JvmStatic
    fun main(args: Array<String>) {
        val workerId = WorkerId("UnluckyInternDoingManualWork")
        val coll = emptyLocalCollection("sample03tasks")
        val dao = FailureSupportingTaskDao(coll)

        dao.submitTask("A")


        val executionId1 = dao.fetchNextTaskExecution(workerId)!!.executionId
        dao.markAsFailedAndCanNOTRetry(executionId1, IllegalStateException("OK. So the building burned down!!! I think I can go home now"))


        displayTinyTasksSummary(
            coll,
            setOf("maxAttemptsCount", "attemptsLeft")
        )


        dao.fetchNextTaskExecution(workerId)
    }
}



object DelayedRetryAfterFailure {

    @JvmStatic
    fun main(args: Array<String>) {
        val workerId = WorkerId("UnluckyInternDoingManualWork")
        val coll = emptyLocalCollection("sample03tasks")
        val dao = FailureSupportingTaskDao(coll)

        dao.submitTask("A")


        val executionId1 = dao.fetchNextTaskExecution(workerId)!!.executionId
        dao.markAsFailed(
            executionId1,
            IllegalStateException("Hmm. So It's not as easy as I thought."),
            Duration.ofSeconds(2)
        )


        dao.fetchNextTaskExecution(workerId)

        displayTinyTasksSummary(
            coll,
            setOf("maxAttemptsCount", "attemptsLeft", "canBeExecutedAsOf")
        )



        Thread.sleep(2_500)


        dao.fetchNextTaskExecution(workerId)

        displayTinyTasksSummary(
            coll,
            setOf("maxAttemptsCount", "attemptsLeft", "canBeExecutedAsOf")
        )
    }
}
