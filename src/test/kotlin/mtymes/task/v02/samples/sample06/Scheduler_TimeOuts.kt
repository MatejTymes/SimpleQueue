package mtymes.task.v02.samples.sample06

import com.mongodb.client.MongoCollection
import mtymes.task.v02.common.mongo.DocBuilder.Companion.doc
import mtymes.task.v02.scheduler.dao.GenericTaskScheduler
import mtymes.task.v02.scheduler.dao.SchedulerDefaults
import mtymes.task.v02.scheduler.domain.ExecutionId
import mtymes.task.v02.scheduler.domain.TaskId
import mtymes.task.v02.scheduler.domain.WorkerId
import mtymes.task.v02.test.mongo.emptyLocalCollection
import mtymes.task.v02.test.task.TaskViewer.displayTinyTasksSummary
import org.bson.Document
import printTimedString
import java.time.Duration


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
            setOf("killableAfter")
        )


        dao.fetchNextTaskExecution(workerId)
        dao.markAsSucceeded(executionId)


        displayTinyTasksSummary(
            coll,
            setOf("killableAfter")
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
            setOf("killableAfter")
        )

        try {
            dao.markAsSucceeded(executionId)
        } catch (e: Exception) {
            printTimedString(e.message!!)
        }

        dao.fetchNextTaskExecution(workerId)

        displayTinyTasksSummary(
            coll,
            setOf("killableAfter")
        )
    }
}


object HeartBeatExtendsKeepAlivePerios {

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
            setOf("killableAfter")
        )
    }
}
