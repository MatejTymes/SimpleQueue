package mtymes.tasks.samples.sample06

import com.mongodb.client.MongoCollection
import mtymes.tasks.common.mongo.DocBuilder.Companion.doc
import mtymes.tasks.scheduler.dao.GenericTaskScheduler
import mtymes.tasks.scheduler.dao.SchedulerDefaults
import mtymes.tasks.scheduler.domain.ExecutionId
import mtymes.tasks.scheduler.domain.TaskId
import mtymes.tasks.scheduler.domain.WorkerId
import mtymes.tasks.test.mongo.emptyLocalCollection
import mtymes.tasks.test.task.TaskViewer.displayTinyTasksSummary
import mtymes.tasks.worker.HeartBeatingWorker
import mtymes.tasks.worker.sweatshop.HumbleSweatShop
import org.bson.Document
import printTimedString
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit


data class TaskToProcess(
    val executionId: ExecutionId,
    val request: String
)


class TimeOutingTasksDao(
    tasksCollection: MongoCollection<Document>
) {
    val scheduler = GenericTaskScheduler(
        collection = tasksCollection,
        defaults = SchedulerDefaults(
            ttlDuration = Duration.ofDays(7),
            afterStartKeepAliveFor = Duration.ofMinutes(5),
            maxAttemptCount = 3
        )
    )

    fun submitTask(
        request: String
    ): TaskId {
        val taskId = scheduler.submitTask(
            doc(
                "request" to request
            )
        )
        printTimedString("submitted Task '${request}'")

        return taskId
    }

    fun fetchNextTaskExecution(
        workerId: WorkerId,
        afterStartKeepAliveFor: Duration = scheduler.defaults.afterStartKeepAliveFor
    ): TaskToProcess? {
        val result = scheduler
            .fetchNextAvailableExecution(
                workerId = workerId,
                keepAliveFor = afterStartKeepAliveFor
            )?.let { summary ->
                TaskToProcess(
                    executionId = summary.execution.executionId,
                    request = summary.task.data.getString("request")
                )
            }

        if (result != null) {
            printTimedString("fetched Execution ${result.executionId} [${result.request}] that should be kept alive for ${afterStartKeepAliveFor}")
        } else {
            printTimedString("did NOT fetch any Execution")
        }

        return result
    }

    fun markAsSucceeded(
        executionId: ExecutionId
    ) {
        val result = scheduler.markAsSucceeded(
            executionId = executionId
        )
        if (result != null) {
            printTimedString("marked Execution ${executionId} as SUCCEEDED")
        } else {
            printTimedString("did NOT mark Execution ${executionId} as SUCCEEDED")
        }
    }

    fun findAndMarkTimedOutTasks(
        retryDelay: Duration = Duration.ofSeconds(0)
    ) {
        printTimedString("searching and Marking TIMED OUT Executions")

        scheduler.findAndMarkTimedOutTasks(
            retryDelay = retryDelay,
            additionalExecutionData = doc(
                "timeoutMessage" to "It took you too long to finish"
            )
        )
    }

    fun registerHeartBeat(
        executionId: ExecutionId,
        keepAliveFor: Duration
    ) {
        val result = scheduler.registerHeartBeat(
            executionId = executionId,
            keepAliveFor = keepAliveFor
        )
        if (result) {
            printTimedString("registered HeartBeat for Execution ${executionId} and increased it's time to be alive by ${keepAliveFor}")
        } else {
            printTimedString("did NOT registered HeartBeat for Execution ${executionId} and NOT increased it's time to be alive by ${keepAliveFor}")
        }
    }
}



object TaskDoesNOTTimeOutAutomatically {

    @JvmStatic
    fun main(args: Array<String>) {
        val workerId = WorkerId("UnluckyInternDoingManualWork")
        val coll = emptyLocalCollection("sample06tasks")
        val dao = TimeOutingTasksDao(coll)

        dao.submitTask("A")

        val executionId = dao.fetchNextTaskExecution(
            workerId = workerId,
            afterStartKeepAliveFor = Duration.ofSeconds(1)
        )!!.executionId

        Thread.sleep(2_000)


        displayTinyTasksSummary(
            coll,
            setOf("executions.lastHeartBeatAt", "executions.timesOutAfter")
        )


        dao.fetchNextTaskExecution(workerId)
        dao.markAsSucceeded(executionId)


        displayTinyTasksSummary(
            coll,
            setOf("executions.lastHeartBeatAt", "executions.timesOutAfter")
        )
    }
}


object CallingTaskToMarkExecutionAsTimedOut {

    @JvmStatic
    fun main(args: Array<String>) {
        val workerId = WorkerId("UnluckyInternDoingManualWork")
        val coll = emptyLocalCollection("sample06tasks")
        val dao = TimeOutingTasksDao(coll)

        dao.submitTask("A")

        val executionId = dao.fetchNextTaskExecution(
            workerId = workerId,
            afterStartKeepAliveFor = Duration.ofSeconds(1)
        )!!.executionId

        Thread.sleep(2_000)


        dao.findAndMarkTimedOutTasks(Duration.ofSeconds(0))


        displayTinyTasksSummary(
            coll,
            setOf("executions.lastHeartBeatAt", "executions.timesOutAfter")
        )

        try {
            dao.markAsSucceeded(executionId)
        } catch (e: Exception) {
            printTimedString(e.message!!)
        }

        dao.fetchNextTaskExecution(workerId)

        displayTinyTasksSummary(
            coll,
            setOf("executions.lastHeartBeatAt", "executions.timesOutAfter")
        )
    }
}


object HeartBeatExtendsKeepAlivePeriod {

    @JvmStatic
    fun main(args: Array<String>) {
        val workerId = WorkerId("UnluckyInternDoingManualWork")
        val coll = emptyLocalCollection("sample06tasks")
        val dao = TimeOutingTasksDao(coll)

        dao.submitTask("A")

        val executionId = dao.fetchNextTaskExecution(
            workerId = workerId,
            afterStartKeepAliveFor = Duration.ofSeconds(2)
        )!!.executionId

        Thread.sleep(1_000)

        dao.registerHeartBeat(
            executionId = executionId,
            keepAliveFor = Duration.ofSeconds(5)
        )

        Thread.sleep(3_000)

        dao.findAndMarkTimedOutTasks(Duration.ofSeconds(0))

        displayTinyTasksSummary(
            coll,
            setOf("executions.lastHeartBeatAt", "executions.timesOutAfter")
        )
    }
}



class LazyHeartBeatingWorker(
    val dao: TimeOutingTasksDao,
    val coll: MongoCollection<Document>
) : HeartBeatingWorker<TaskToProcess> {

    override fun heartBeatInterval(task: TaskToProcess, workerId: WorkerId): Duration {
        return Duration.ofSeconds(2)
    }

    override fun updateHeartBeat(task: TaskToProcess, workerId: WorkerId) {
        dao.registerHeartBeat(
            executionId = task.executionId,
            keepAliveFor = Duration.ofSeconds(3)
        )

        displayTinyTasksSummary(
            coll,
            setOf("executions.lastHeartBeatAt", "executions.timesOutAfter")
        )
    }

    override fun fetchNextTaskToProcess(workerId: WorkerId): TaskToProcess? {
        return dao.fetchNextTaskExecution(
            workerId = workerId,
            afterStartKeepAliveFor = Duration.ofSeconds(3)
        )
    }

    override fun executeTask(task: TaskToProcess, workerId: WorkerId) {
        // some complex sleeping
        Thread.sleep(7_000)

        dao.markAsSucceeded(task.executionId)
    }

    override fun taskToLoggableString(task: TaskToProcess, workerId: WorkerId): String {
        return task.request
    }
}



object WorkerWithHeartBeatShowcase {

    @JvmStatic
    fun main(args: Array<String>) {
        val coll = emptyLocalCollection("sample06tasks")
        val dao = TimeOutingTasksDao(coll)

        dao.submitTask("A")

        val executor = Executors.newScheduledThreadPool(1)

        executor.scheduleAtFixedRate({ dao.findAndMarkTimedOutTasks() }, 0, 1, TimeUnit.SECONDS)

        try {
            HumbleSweatShop().use { sweatShop ->

                val worker = LazyHeartBeatingWorker(dao, coll)

                sweatShop.addAndStartWorker(worker)

                Thread.sleep(8_500)
            }
        } finally {
            executor.shutdownNow()
        }

        displayTinyTasksSummary(
            coll,
            setOf("executions.lastHeartBeatAt", "executions.timesOutAfter")
        )
    }
}
