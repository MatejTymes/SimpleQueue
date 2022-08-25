package mtymes.tasks.samples.sample08

import com.mongodb.client.MongoCollection
import mtymes.tasks.common.mongo.DocBuilder.Companion.doc
import mtymes.tasks.scheduler.dao.GenericTaskScheduler
import mtymes.tasks.scheduler.dao.SchedulerDefaults
import mtymes.tasks.scheduler.domain.ExecutionId
import mtymes.tasks.scheduler.domain.TaskId
import mtymes.tasks.scheduler.domain.WorkerId
import mtymes.tasks.test.mongo.emptyLocalCollection
import org.bson.Document
import printTimedString
import java.time.Duration

data class TaskToProcess(
    val executionId: ExecutionId,
    val request: String,
    val group: String
)


class CustomQueryTasksDao(
    tasksCollection: MongoCollection<Document>
) {
    val scheduler = GenericTaskScheduler(
        collection = tasksCollection,
        defaults = SchedulerDefaults(
            ttlDuration = Duration.ofDays(7),
            afterStartKeepAliveFor = Duration.ofMinutes(5)
        )
    )

    fun submitTask(
        request: String,
        group: String
    ): TaskId {
        val taskId = scheduler.submitTask(
            doc(
                "request" to request,
                "group" to group
            )
        )
        printTimedString("submitted Task '${request}' for Group ${group}")

        return taskId
    }

    fun fetchNextTaskExecution(
        workerId: WorkerId,
        forGroup: String
    ): TaskToProcess? {
        val result = scheduler
            .fetchNextAvailableExecution(
                workerId = workerId,
                additionalConstraint = doc(
                    "data.group" to forGroup
                )
            )?.let { summary ->
                TaskToProcess(
                    executionId = summary.execution.executionId,
                    request = summary.task.data.getString("request"),
                    group = summary.task.data.getString("group")
                )
            }

        if (result != null) {
            printTimedString("fetched Execution ${result.executionId} [${result.request}] for Group ${forGroup}")
        } else {
            printTimedString("did NOT fetch any Execution for Group ${forGroup}")
        }

        return result
    }
}



object FetchTasksBasedOnCustomQuery {

    @JvmStatic
    fun main(args: Array<String>) {
        val workerId = WorkerId("UnluckyInternDoingManualWork")
        val coll = emptyLocalCollection("sample08tasks")
        val dao = CustomQueryTasksDao(coll)


        dao.submitTask("A", "group1")
        dao.submitTask("B", "group4")
        dao.submitTask("C", "group4")
        dao.submitTask("D", "group1")


        dao.fetchNextTaskExecution(workerId, "group1")
        dao.fetchNextTaskExecution(workerId, "group1")
        dao.fetchNextTaskExecution(workerId, "group1")

        dao.fetchNextTaskExecution(workerId, "group4")
        dao.fetchNextTaskExecution(workerId, "group4")
        dao.fetchNextTaskExecution(workerId, "group4")
    }
}