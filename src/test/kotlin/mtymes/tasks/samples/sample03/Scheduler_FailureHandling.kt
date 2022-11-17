package mtymes.tasks.samples.sample03

import com.mongodb.client.MongoCollection
import mtymes.tasks.common.domain.WorkerId
import mtymes.tasks.common.mongo.DocBuilder.Companion.doc
import mtymes.tasks.common.time.Durations
import mtymes.tasks.scheduler.dao.GenericScheduler
import mtymes.tasks.scheduler.dao.SchedulerDefaults
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.CAN_BE_EXECUTED_AS_OF
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.EXECUTIONS_COUNT
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.EXECUTION_ATTEMPTS_LEFT
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.FINISHED_AT
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.LAST_EXECUTION
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.PREVIOUS_EXECUTIONS
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.STARTED_AT
import mtymes.tasks.scheduler.domain.*
import mtymes.tasks.test.mongo.emptyLocalCollection
import mtymes.tasks.test.task.TaskViewer.displayTinyTasksSummary
import mtymes.tasks.worker.Worker
import mtymes.tasks.worker.sweatshop.HumbleSweatShop
import mtymes.tasks.worker.sweatshop.ShutDownMode.OnceNoMoreWork
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
    val scheduler = GenericScheduler(
        collection = tasksCollection,
        defaults = SchedulerDefaults(
            submitTaskOptions = SubmitTaskOptions(
                ttl = Durations.SEVEN_DAYS,
                maxAttemptsCount = 3
            ),

            pickNextExecutionOptions = PickNextExecutionOptions(
                keepAliveFor = Durations.FIVE_MINUTES
            ),

            markAsFailedButCanRetryOptions = MarkAsFailedButCanRetryOptions(
                retryDelay = Durations.ZERO_SECONDS
            )
        )
    )

    fun submitTask(
        request: String,
        retainOnlyLastExecution: Boolean = false
    ): TaskId {
        val taskId = scheduler.submitTask(
            customData = doc("request" to request),
            options = scheduler.defaults.submitTaskOptions!!.copy(
                retainOnlyLastExecution = retainOnlyLastExecution
            )
        )
        printTimedString("submitted Task '${request}'")
        return taskId
    }

    fun findTask(
        taskId: TaskId
    ): Task? {
        return scheduler.findTask(taskId)
    }

    fun pickNextTaskExecution(
        workerId: WorkerId
    ): TaskToProcess? {
        val result = scheduler.pickNextAvailableExecution(workerId)
            ?.let { summary ->
                TaskToProcess(
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
            options = scheduler.defaults.markAsFailedButCanRetryOptions!!.copy(
                retryDelay = retryDelay
            ),
            additionalExecutionData = doc(
                "failureMessage" to e.message
            )
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

    override fun pickNextTaskToProcess(workerId: WorkerId): TaskToProcess? {
        return dao.pickNextTaskExecution(workerId)
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

            sweatShop.close(
                shutDownMode = OnceNoMoreWork,
                waitTillDone = true
            )
        }

        displayTinyTasksSummary(
            coll, setOf(
//                MAX_EXECUTIONS_COUNT,
                EXECUTION_ATTEMPTS_LEFT,
                EXECUTIONS_COUNT,
                PREVIOUS_EXECUTIONS + "." + STARTED_AT,
                PREVIOUS_EXECUTIONS + "." + FINISHED_AT,
                LAST_EXECUTION + "." + STARTED_AT,
                LAST_EXECUTION + "." + FINISHED_AT,
            )
        )
    }
}

object WorkerFailingButRetainingOnlyLastExecution {

    @JvmStatic
    fun main(args: Array<String>) {
        val coll = emptyLocalCollection("sample03tasks")
        val dao = FailureSupportingTaskDao(coll)

        val taskId = dao.submitTask(
            request = "A",
            retainOnlyLastExecution = true
        )

        HumbleSweatShop().use { sweatShop ->

            val worker = BrokenWorker(dao)

            sweatShop.addAndStartWorker(worker)

            sweatShop.close(
                shutDownMode = OnceNoMoreWork,
                waitTillDone = true
            )
        }

        displayTinyTasksSummary(
            coll, setOf(
//                MAX_EXECUTIONS_COUNT,
                EXECUTION_ATTEMPTS_LEFT,
                EXECUTIONS_COUNT,
                PREVIOUS_EXECUTIONS + "." + STARTED_AT,
                PREVIOUS_EXECUTIONS + "." + FINISHED_AT,
                LAST_EXECUTION + "." + STARTED_AT,
                LAST_EXECUTION + "." + FINISHED_AT,
            )
        )

        dao.findTask(taskId)?.let {
            println("allExecutions = " + it.allExecutions)
        }
    }
}


object FailThenSucceed {

    @JvmStatic
    fun main(args: Array<String>) {
        val workerId = WorkerId("UnluckyInternDoingManualWork")
        val coll = emptyLocalCollection("sample03tasks")
        val dao = FailureSupportingTaskDao(coll)

        dao.submitTask("A")


        val executionId1 = dao.pickNextTaskExecution(workerId)!!.executionId
        dao.markAsFailed(executionId1, IllegalStateException("It should have worked"))


        displayTinyTasksSummary(
            coll, setOf(
//                MAX_EXECUTIONS_COUNT,
                EXECUTION_ATTEMPTS_LEFT,
                EXECUTIONS_COUNT
            )
        )


        val executionId2 = dao.pickNextTaskExecution(workerId)!!.executionId
        dao.markAsSucceeded(executionId2, "So glad it's over. I'm not doing this again")


        displayTinyTasksSummary(
            coll, setOf(
//                MAX_EXECUTIONS_COUNT,
                EXECUTION_ATTEMPTS_LEFT,
                EXECUTIONS_COUNT
            )
        )


        dao.pickNextTaskExecution(workerId)
    }
}


object UnrecoverableFailure {

    @JvmStatic
    fun main(args: Array<String>) {
        val workerId = WorkerId("UnluckyInternDoingManualWork")
        val coll = emptyLocalCollection("sample03tasks")
        val dao = FailureSupportingTaskDao(coll)

        dao.submitTask("A")


        val executionId1 = dao.pickNextTaskExecution(workerId)!!.executionId
        dao.markAsFailedAndCanNOTRetry(
            executionId1,
            IllegalStateException("OK. So the building burned down!!! I think I can go home now")
        )


        displayTinyTasksSummary(
            coll, setOf(
//                MAX_EXECUTIONS_COUNT,
                EXECUTION_ATTEMPTS_LEFT,
                EXECUTIONS_COUNT
            )
        )


        dao.pickNextTaskExecution(workerId)
    }
}


object DelayedRetryAfterFailure {

    @JvmStatic
    fun main(args: Array<String>) {
        val workerId = WorkerId("UnluckyInternDoingManualWork")
        val coll = emptyLocalCollection("sample03tasks")
        val dao = FailureSupportingTaskDao(coll)

        dao.submitTask("A")


        val executionId1 = dao.pickNextTaskExecution(workerId)!!.executionId
        dao.markAsFailed(
            executionId1,
            IllegalStateException("Hmm. So It's not as easy as I thought."),
            Duration.ofSeconds(2)
        )


        dao.pickNextTaskExecution(workerId)

        displayTinyTasksSummary(
            coll, setOf(
//                MAX_EXECUTIONS_COUNT,
                EXECUTION_ATTEMPTS_LEFT,
                EXECUTIONS_COUNT,
                CAN_BE_EXECUTED_AS_OF,
                STARTED_AT,
                PREVIOUS_EXECUTIONS + "." + STARTED_AT,
                LAST_EXECUTION + "." + STARTED_AT,
            )
        )


        Thread.sleep(2_500)


        dao.pickNextTaskExecution(workerId)

        displayTinyTasksSummary(
            coll, setOf(
//                MAX_EXECUTIONS_COUNT,
                EXECUTION_ATTEMPTS_LEFT,
                EXECUTIONS_COUNT,
                CAN_BE_EXECUTED_AS_OF,
                STARTED_AT,
                PREVIOUS_EXECUTIONS + "." + STARTED_AT,
                LAST_EXECUTION + "." + STARTED_AT,
            )
        )
    }
}
