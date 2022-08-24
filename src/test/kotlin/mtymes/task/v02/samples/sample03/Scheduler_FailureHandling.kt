package mtymes.task.v02.samples.sample03

import com.mongodb.client.MongoCollection
import mtymes.task.v02.common.mongo.DocBuilder.Companion.doc
import mtymes.task.v02.common.time.UTCClock
import mtymes.task.v02.scheduler.dao.GenericTaskScheduler
import mtymes.task.v02.scheduler.dao.SchedulerDefaults
import mtymes.task.v02.scheduler.domain.ExecutionId
import mtymes.task.v02.scheduler.domain.WorkerId
import mtymes.task.v02.test.mongo.emptyLocalCollection
import mtymes.task.v02.test.task.TaskViewer.displayTinyTasksSummary
import mtymes.task.v02.worker.Worker
import mtymes.task.v02.worker.sweatshop.HumbleSweatShop
import org.bson.Document
import java.time.Duration
import java.util.*


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
    }

    fun fetchNextTaskExecution(
        workerId: WorkerId
    ): TaskToProcess? {
        return scheduler.fetchNextAvailableExecution(workerId)
            ?.let { summary ->
                TaskToProcess(
                    executionId = summary.execution.executionId,
                    request = summary.task.data.getString(REQUEST)
                )
            }
    }

    fun markAsSucceeded(
        executionId: ExecutionId,
        message: String
    ) {
        scheduler.markAsSucceeded(
            executionId = executionId,
            additionalExecutionData = doc(
                "successMessage" to message
            )
        )
    }

    fun markAsFailed(
        executionId: ExecutionId,
        e: Exception
    ) {
        scheduler.markAsFailedButCanRetry(
            executionId = executionId,
            additionalExecutionData = doc(
                "failureMessage" to e.message
            )
        )
    }

    fun markAsFailedAndCanNOTRetry(
        executionId: ExecutionId,
        e: Exception
    ) {
        scheduler.markAsFailedButCanNOTRetry(
            executionId = executionId,
            additionalExecutionData = doc(
                "failureMessage" to e.message
            )
        )
    }

    fun markAsFailed(
        executionId: ExecutionId,
        e: Exception,
        retryDelay: Duration
    ) {
        scheduler.markAsFailedButCanRetry(
            executionId = executionId,
            additionalExecutionData = doc(
                "failureMessage" to e.message
            ),
            retryDelay = retryDelay
        )
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

            Thread.sleep(5_000)
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

        val executionId2 = dao.fetchNextTaskExecution(workerId)!!.executionId
        dao.markAsSucceeded(executionId2, "So glad it's over. I'm not doing this again")


        displayTinyTasksSummary(
            coll,
            setOf("maxAttemptsCount", "attemptsLeft")
        )


        val isThereAnythingLeft = dao.fetchNextTaskExecution(workerId) != null
        println("isThereAnythingLeft = ${isThereAnythingLeft}")
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


        val isThereAnythingLeft = dao.fetchNextTaskExecution(workerId) != null
        println("isThereAnythingLeft = ${isThereAnythingLeft}")
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
            IllegalStateException("Hmm. So I't not as easy as I thought."),
            Duration.ofSeconds(2)
        )


        var fetchedNext = dao.fetchNextTaskExecution(workerId)

        printCurrentTime()
        displayTinyTasksSummary(
            coll,
            setOf("maxAttemptsCount", "attemptsLeft", "canBeExecutedAsOf")
        )
        println("isThereAnythingAvailable = ${fetchedNext != null}")


        Thread.sleep(2_500)


        fetchedNext = dao.fetchNextTaskExecution(workerId)

        printCurrentTime()
        displayTinyTasksSummary(
            coll,
            setOf("maxAttemptsCount", "attemptsLeft", "canBeExecutedAsOf")
        )
        println("isThereAnythingAvailable = ${fetchedNext != null}")
    }

    private fun printCurrentTime() {
        println("\n\nnow = ${Date.from(UTCClock().now().toInstant())}")
    }
}
