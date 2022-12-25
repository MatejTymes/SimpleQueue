package mtymes.tasks.samples.sample06

import com.mongodb.client.MongoCollection
import mtymes.tasks.common.domain.WorkerId
import mtymes.tasks.common.mongo.builder.WithCoreDocumentBuilder
import mtymes.tasks.common.time.Durations
import mtymes.tasks.scheduler.dao.GenericScheduler
import mtymes.tasks.scheduler.dao.SchedulerDefaults
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.HEARTBEAT_AT
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.KILLABLE_AFTER
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.LAST_EXECUTION
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.PREVIOUS_EXECUTIONS
import mtymes.tasks.scheduler.domain.*
import mtymes.tasks.test.mongo.emptyLocalCollection
import mtymes.tasks.test.task.TaskViewer.displayTinyTasksSummary
import mtymes.tasks.worker.HeartBeatingWorker
import mtymes.tasks.worker.sweatshop.HumbleSweatShop
import mtymes.tasks.worker.sweatshop.ShutDownMode.OnceNoMoreWork
import org.bson.Document
import printTimedString
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit


data class TaskToProcess(
    val executionId: ExecutionId,
    val request: String
)


class KillingTasksDao(
    tasksCollection: MongoCollection<Document>
) : WithCoreDocumentBuilder {
    val scheduler = GenericScheduler(
        collection = tasksCollection,
        defaults = SchedulerDefaults(

            submitTaskOptions = SubmitTaskOptions(
                ttl = Durations.SEVEN_DAYS,
                maxAttemptsCount = 3
            ),

            pickNextExecutionOptions = PickNextExecutionOptions(
                keepAliveFor = Durations.FIVE_MINUTES
            )
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

    fun pickNextTaskExecution(
        workerId: WorkerId,
        afterStartKeepAliveFor: Duration = scheduler.defaults.pickNextExecutionOptions!!.keepAliveFor
    ): TaskToProcess? {
        val result = scheduler
            .pickNextAvailableExecution(
                workerId = workerId,
                options = scheduler.defaults.pickNextExecutionOptions!!.copy(
                    keepAliveFor = afterStartKeepAliveFor
                )
            )?.let { summary ->
                TaskToProcess(
                    executionId = summary.pickedExecution.executionId,
                    request = summary.underlyingTask.data().getString("request")
                )
            }

        if (result != null) {
            printTimedString("picked Execution ${result.executionId} [${result.request}] that should be kept alive for ${afterStartKeepAliveFor}")
        } else {
            printTimedString("did NOT pick any Execution")
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

    fun findAndMarkKillableExecutions(
        retryDelay: Duration = Duration.ofSeconds(0)
    ) {
        printTimedString("searching and Marking DEAD Executions")

        val killedCount = scheduler.markKillableExecutionsAsDead(
            options = MarkKillableExecutionsAsDeadOptions(
                retryDelay = retryDelay
            ),
            additionalExecutionData = doc(
                "timeoutMessage" to "It took you too long to finish"
            )
        )

        printTimedString("Killed ${ killedCount } Execution/s")
    }

    fun registerHeartBeat(
        executionId: ExecutionId,
        keepAliveFor: Duration
    ) {
        val result = scheduler.registerHeartBeat(
            executionId = executionId,
            options = RegisterHeartBeatOptions(
                keepAliveFor = keepAliveFor
            )
        )
        if (result) {
            printTimedString("registered HeartBeat for Execution ${executionId} and increased it's time to be alive by ${keepAliveFor}")
        } else {
            printTimedString("did NOT registered HeartBeat for Execution ${executionId} and NOT increased it's time to be alive by ${keepAliveFor}")
        }
    }
}


object TaskDoesNOTDieAutomatically {

    @JvmStatic
    fun main(args: Array<String>) {
        val workerId = WorkerId("UnluckyInternDoingManualWork")
        val coll = emptyLocalCollection("sample06tasks")
        val dao = KillingTasksDao(coll)

        dao.submitTask("A")

        val executionId = dao.pickNextTaskExecution(
            workerId = workerId,
            afterStartKeepAliveFor = Duration.ofSeconds(1)
        )!!.executionId

        Thread.sleep(2_000)


        displayTinyTasksSummary(coll, setOf(
            PREVIOUS_EXECUTIONS + "." + HEARTBEAT_AT,
            PREVIOUS_EXECUTIONS + "." + KILLABLE_AFTER,
            LAST_EXECUTION + "." + HEARTBEAT_AT,
            LAST_EXECUTION + "." + KILLABLE_AFTER
        ))


        dao.pickNextTaskExecution(workerId)
        dao.markAsSucceeded(executionId)


        displayTinyTasksSummary(coll, setOf(
            PREVIOUS_EXECUTIONS + "." + HEARTBEAT_AT,
            PREVIOUS_EXECUTIONS + "." + KILLABLE_AFTER,
            LAST_EXECUTION + "." + HEARTBEAT_AT,
            LAST_EXECUTION + "." + KILLABLE_AFTER
        ))
    }
}


object CallingTaskToMarkExecutionAsDead {

    @JvmStatic
    fun main(args: Array<String>) {
        val workerId = WorkerId("UnluckyInternDoingManualWork")
        val coll = emptyLocalCollection("sample06tasks")
        val dao = KillingTasksDao(coll)

        dao.submitTask("A")

        val executionId = dao.pickNextTaskExecution(
            workerId = workerId,
            afterStartKeepAliveFor = Duration.ofSeconds(1)
        )!!.executionId

        Thread.sleep(2_000)


        dao.findAndMarkKillableExecutions(Duration.ofSeconds(0))


        displayTinyTasksSummary(coll, setOf(
            PREVIOUS_EXECUTIONS + "." + HEARTBEAT_AT,
            PREVIOUS_EXECUTIONS + "." + KILLABLE_AFTER,
            LAST_EXECUTION + "." + HEARTBEAT_AT,
            LAST_EXECUTION + "." + KILLABLE_AFTER
        ))

        try {
            dao.markAsSucceeded(executionId)
        } catch (e: Exception) {
            printTimedString(e.message!!)
        }

        dao.pickNextTaskExecution(workerId)

        displayTinyTasksSummary(coll, setOf(
            PREVIOUS_EXECUTIONS + "." + HEARTBEAT_AT,
            PREVIOUS_EXECUTIONS + "." + KILLABLE_AFTER,
            LAST_EXECUTION + "." + HEARTBEAT_AT,
            LAST_EXECUTION + "." + KILLABLE_AFTER
        ))
    }
}


object HeartBeatExtendsKeepAlivePeriod {

    @JvmStatic
    fun main(args: Array<String>) {
        val workerId = WorkerId("UnluckyInternDoingManualWork")
        val coll = emptyLocalCollection("sample06tasks")
        val dao = KillingTasksDao(coll)

        dao.submitTask("A")

        val executionId = dao.pickNextTaskExecution(
            workerId = workerId,
            afterStartKeepAliveFor = Duration.ofSeconds(2)
        )!!.executionId

        Thread.sleep(1_000)

        dao.registerHeartBeat(
            executionId = executionId,
            keepAliveFor = Duration.ofSeconds(5)
        )

        Thread.sleep(3_000)

        dao.findAndMarkKillableExecutions(Duration.ofSeconds(0))

        displayTinyTasksSummary(coll, setOf(
            PREVIOUS_EXECUTIONS + "." + HEARTBEAT_AT,
            PREVIOUS_EXECUTIONS + "." + KILLABLE_AFTER,
            LAST_EXECUTION + "." + HEARTBEAT_AT,
            LAST_EXECUTION + "." + KILLABLE_AFTER
        ))
    }
}


class LazyHeartBeatingWorker(
    val dao: KillingTasksDao,
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

        displayTinyTasksSummary(coll, setOf(
            PREVIOUS_EXECUTIONS + "." + HEARTBEAT_AT,
            PREVIOUS_EXECUTIONS + "." + KILLABLE_AFTER,
            LAST_EXECUTION + "." + HEARTBEAT_AT,
            LAST_EXECUTION + "." + KILLABLE_AFTER
        ))
    }

    override fun pickNextTaskToProcess(workerId: WorkerId): TaskToProcess? {
        return dao.pickNextTaskExecution(
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
        val dao = KillingTasksDao(coll)

        dao.submitTask("A")

        val executor = Executors.newScheduledThreadPool(1)

        executor.scheduleAtFixedRate({ dao.findAndMarkKillableExecutions() }, 0, 1, TimeUnit.SECONDS)

        try {
            HumbleSweatShop().use { sweatShop ->

                val worker = LazyHeartBeatingWorker(dao, coll)

                sweatShop.addAndStartWorker(worker)

                sweatShop.close(
                    shutDownMode = OnceNoMoreWork,
                    waitTillDone = true
                )
            }
        } finally {
            executor.shutdownNow()
        }

        displayTinyTasksSummary(coll, setOf(
            PREVIOUS_EXECUTIONS + "." + HEARTBEAT_AT,
            PREVIOUS_EXECUTIONS + "." + KILLABLE_AFTER,
            LAST_EXECUTION + "." + HEARTBEAT_AT,
            LAST_EXECUTION + "." + KILLABLE_AFTER
        ))
    }
}
