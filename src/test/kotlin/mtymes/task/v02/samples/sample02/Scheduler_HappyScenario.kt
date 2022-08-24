package mtymes.task.v02.samples.sample02

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



class SimpleTaskDao(
    tasksCollection: MongoCollection<Document>
) {
    val scheduler = GenericTaskScheduler(
        collection = tasksCollection,
        defaults = SchedulerDefaults(
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
