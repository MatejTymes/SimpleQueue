package mtymes.tasks.samples.sample02

import com.mongodb.client.MongoCollection
import mtymes.tasks.common.mongo.DocBuilder.Companion.doc
import mtymes.tasks.common.time.Durations.FIVE_MINUTES
import mtymes.tasks.common.time.Durations.SEVEN_DAYS
import mtymes.tasks.scheduler.dao.GenericTaskScheduler
import mtymes.tasks.scheduler.dao.SchedulerDefaults
import mtymes.tasks.scheduler.domain.ExecutionId
import mtymes.tasks.scheduler.domain.FetchNextExecutionOptions
import mtymes.tasks.scheduler.domain.SubmitTaskOptions
import mtymes.tasks.scheduler.domain.WorkerId
import mtymes.tasks.test.mongo.emptyLocalCollection
import mtymes.tasks.test.task.TaskViewer.displayTinyTasksSummary
import mtymes.tasks.worker.Worker
import mtymes.tasks.worker.sweatshop.HumbleSweatShop
import org.bson.Document
import printTimedString


data class TaskToProcess(
    val executionId: ExecutionId,
    val request: String
)



class SimpleTaskDao(
    tasksCollection: MongoCollection<Document>
) {
    val scheduler = GenericTaskScheduler(
        collection = tasksCollection,
        defaults = SchedulerDefaults(

            submitTaskOptions = SubmitTaskOptions(
                ttl = SEVEN_DAYS
            ),

            fetchNextExecutionOptions = FetchNextExecutionOptions(
                keepAliveFor = FIVE_MINUTES
            )
        )
    )

    fun submitTask(
        request: String
    ) {
        scheduler.submitTask(
            doc("request" to request)
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
                    request = summary.task.data.getString("request")
                )
            }

        if (result != null) {
            printTimedString("fetched Execution ${result.executionId} [${result.request}]")
        } else {
            printTimedString("did NOT fetch any Execution")
        }

        return result
    }

    fun markAsSucceeded(
        executionId: ExecutionId
    ) {
        val result = scheduler.markAsSucceeded(executionId)
        if (result != null) {
            printTimedString("marked Execution ${executionId} as SUCCEEDED")
        } else {
            printTimedString("did NOT mark Execution ${executionId} as SUCCEEDED")
        }
    }
}



open class SimpleWorker(
    val dao: SimpleTaskDao
) : Worker<TaskToProcess> {

    override fun fetchNextTaskToProcess(
        workerId: WorkerId
    ): TaskToProcess? {
        return dao.fetchNextTaskExecution(workerId)
    }

    override fun executeTask(task: TaskToProcess, workerId: WorkerId) {
        // i'm lazy and just pretending i do something
        Thread.sleep(1_000)

        dao.markAsSucceeded(task.executionId)
    }
}



object WorkerDoingWork {

    @JvmStatic
    fun main(args: Array<String>) {
        val coll = emptyLocalCollection("sample02tasks")

        val dao = SimpleTaskDao(coll)

        dao.submitTask("A")
        dao.submitTask("B")
        dao.submitTask("C")

        HumbleSweatShop().use { sweatShop ->

            val worker = SimpleWorker(dao)

            sweatShop.addAndStartWorker(worker)

            Thread.sleep(4_000)
        }
    }
}



class ReadableSimpleWorker(
    dao: SimpleTaskDao
) : SimpleWorker(dao) {

    override fun taskToLoggableString(task: TaskToProcess, workerId: WorkerId): String {
        return task.request
    }
}



object MoreReadableWorkerDoingWork {

    @JvmStatic
    fun main(args: Array<String>) {
        val coll = emptyLocalCollection("sample02tasks")

        val dao = SimpleTaskDao(coll)

        dao.submitTask("A")
        dao.submitTask("B")
        dao.submitTask("C")

        HumbleSweatShop().use { sweatShop ->

            val worker = ReadableSimpleWorker(dao)

            sweatShop.addAndStartWorker(worker)

            Thread.sleep(4_000)
        }
    }
}


object MultipleWorkersRegistered {

    @JvmStatic
    fun main(args: Array<String>) {
        val coll = emptyLocalCollection("sample02tasks")

        val dao = SimpleTaskDao(coll)

        dao.submitTask("A")
        dao.submitTask("B")
        dao.submitTask("C")

        HumbleSweatShop().use { sweatShop ->

            val worker1 = ReadableSimpleWorker(dao)
            val worker2 = ReadableSimpleWorker(dao)

            sweatShop.addAndStartWorker(worker1)
            sweatShop.addAndStartWorker(worker2)

            Thread.sleep(4_000)
        }
    }
}



object OneWorkerRegisteredMultipleTimes {

    @JvmStatic
    fun main(args: Array<String>) {
        val coll = emptyLocalCollection("sample02tasks")

        val dao = SimpleTaskDao(coll)

        dao.submitTask("A")
        dao.submitTask("B")
        dao.submitTask("C")

        HumbleSweatShop().use { sweatShop ->

            val worker = ReadableSimpleWorker(dao)

            sweatShop.addAndStartWorker(worker)
            sweatShop.addAndStartWorker(worker)

            Thread.sleep(4_000)
        }

        displayTinyTasksSummary(coll, setOf("executions.workerId"))
    }
}
