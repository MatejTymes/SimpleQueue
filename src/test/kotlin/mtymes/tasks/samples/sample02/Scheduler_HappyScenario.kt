package mtymes.tasks.samples.sample02

import com.mongodb.client.MongoCollection
import mtymes.tasks.common.domain.WorkerId
import mtymes.tasks.common.mongo.builder.WithCoreDocBuilder
import mtymes.tasks.common.time.Durations.FIVE_MINUTES
import mtymes.tasks.common.time.Durations.SEVEN_DAYS
import mtymes.tasks.scheduler.dao.GenericScheduler
import mtymes.tasks.scheduler.dao.SchedulerDefaults
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.LAST_EXECUTION
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.WORKER_ID
import mtymes.tasks.scheduler.domain.ExecutionId
import mtymes.tasks.scheduler.domain.PickNextExecutionOptions
import mtymes.tasks.scheduler.domain.SubmitTaskOptions
import mtymes.tasks.test.mongo.emptyLocalCollection
import mtymes.tasks.test.task.TaskViewer.displayTinyTasksSummary
import mtymes.tasks.worker.Worker
import mtymes.tasks.worker.sweatshop.HumbleSweatShop
import mtymes.tasks.worker.sweatshop.ShutDownMode.OnceNoMoreWork
import org.bson.Document
import printTimedString


data class TaskToProcess(
    val executionId: ExecutionId,
    val request: String
)


class SimpleTaskDao(
    tasksCollection: MongoCollection<Document>
) : WithCoreDocBuilder {
    val scheduler = GenericScheduler(
        collection = tasksCollection,
        defaults = SchedulerDefaults(

            submitTaskOptions = SubmitTaskOptions(
                ttl = SEVEN_DAYS
            ),

            pickNextExecutionOptions = PickNextExecutionOptions(
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
            printTimedString("picked Execution ${result.executionId} [${result.request}]")
        } else {
            printTimedString("did NOT pick any Execution")
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

    override fun pickAvailableWork(
        workerId: WorkerId
    ): TaskToProcess? {
        return dao.pickNextTaskExecution(workerId)
    }

    override fun processWork(work: TaskToProcess, workerId: WorkerId) {
        // i'm lazy and just pretending i do something
        Thread.sleep(1_000)

        dao.markAsSucceeded(work.executionId)
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

            sweatShop.close(
                shutDownMode = OnceNoMoreWork,
                waitTillDone = true
            )
        }
    }
}


class ReadableSimpleWorker(
    dao: SimpleTaskDao
) : SimpleWorker(dao) {

    override fun workToLoggableString(work: TaskToProcess, workerId: WorkerId): String {
        return work.request
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

            sweatShop.close(
                shutDownMode = OnceNoMoreWork,
                waitTillDone = true
            )
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

            sweatShop.close(
                shutDownMode = OnceNoMoreWork,
                waitTillDone = true
            )
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

            sweatShop.close(
                shutDownMode = OnceNoMoreWork,
                waitTillDone = true
            )
        }

        displayTinyTasksSummary(
            coll, setOf(
                "${LAST_EXECUTION}.${WORKER_ID}"
            )
        )
    }
}
