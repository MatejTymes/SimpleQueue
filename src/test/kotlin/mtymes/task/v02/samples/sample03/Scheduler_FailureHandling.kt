package mtymes.task.v02.samples.sample03

import com.mongodb.client.MongoCollection
import mtymes.task.v02.common.mongo.DocBuilder
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
            DocBuilder.doc(REQUEST to request)
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
        val coll = emptyLocalCollection("sample03tasks")

        val dao = FailureSupportingTaskDao(coll)

        dao.submitTask("A")

        val workerId = WorkerId("UnluckyInternDoingManualWork")


        val executionId1 = dao.fetchNextTaskExecution(workerId)!!.executionId
        dao.markAsFailed(executionId1, IllegalStateException("It should have worked"))

        val executionId2 = dao.fetchNextTaskExecution(workerId)!!.executionId
        dao.markAsSucceeded(executionId2, "So glad it's over. I'm not doing this again")

        val isThereAnythingLeft = dao.fetchNextTaskExecution(workerId) != null
        println("isThereAnythingLeft = ${isThereAnythingLeft}")


        displayTinyTasksSummary(
            coll,
            setOf("maxAttemptsCount", "attemptsLeft")
        )
    }
}



object UnrecoverableFailure {

    @JvmStatic
    fun main(args: Array<String>) {
        val coll = emptyLocalCollection("sample03tasks")

        val dao = FailureSupportingTaskDao(coll)

        dao.submitTask("A")

        val workerId = WorkerId("UnluckyInternDoingManualWork")


        val executionId1 = dao.fetchNextTaskExecution(workerId)!!.executionId
        dao.markAsFailedAndCanNOTRetry(executionId1, IllegalStateException("OK. So the building burned down!!! I think I can go home now"))

        val isThereAnythingLeft = dao.fetchNextTaskExecution(workerId) != null
        println("isThereAnythingLeft = ${isThereAnythingLeft}")


        displayTinyTasksSummary(
            coll,
            setOf("maxAttemptsCount", "attemptsLeft")
        )
    }
}



// todo: mtymes - implement the rest

