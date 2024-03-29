package mtymes.tasks.samples.sample08

import com.mongodb.client.MongoCollection
import mtymes.tasks.common.domain.WorkerId
import mtymes.tasks.common.mongo.builder.WithBaseDocBuilder
import mtymes.tasks.common.time.Durations
import mtymes.tasks.scheduler.dao.GenericScheduler
import mtymes.tasks.scheduler.dao.SchedulerDefaults
import mtymes.tasks.scheduler.domain.ExecutionId
import mtymes.tasks.scheduler.domain.PickNextExecutionOptions
import mtymes.tasks.scheduler.domain.SubmitTaskOptions
import mtymes.tasks.scheduler.domain.TaskId
import mtymes.tasks.test.mongo.emptyLocalCollection
import org.bson.Document
import printTimedString

data class TaskToProcess(
    val executionId: ExecutionId,
    val request: String,
    val group: String
)


class CustomQueryTasksDao(
    tasksCollection: MongoCollection<Document>
) : WithBaseDocBuilder {
    val scheduler = GenericScheduler(
        collection = tasksCollection,
        defaults = SchedulerDefaults(

            submitTaskOptions = SubmitTaskOptions(
                ttl = Durations.SEVEN_DAYS
            ),

            pickNextExecutionOptions = PickNextExecutionOptions(
                keepAliveFor = Durations.FIVE_MINUTES
            )
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

    fun pickNextTaskExecution(
        workerId: WorkerId,
        forGroup: String
    ): TaskToProcess? {
        val result = scheduler
            .pickNextAvailableExecution(
                workerId = workerId,
                additionalConstraints = doc(
                    "data.group" to forGroup
                )
            )?.let { summary ->
                TaskToProcess(
                    executionId = summary.pickedExecution.executionId,
                    request = summary.underlyingTask.data().getString("request"),
                    group = summary.underlyingTask.data().getString("group")
                )
            }

        if (result != null) {
            printTimedString("picked Execution ${result.executionId} [${result.request}] for Group ${forGroup}")
        } else {
            printTimedString("did NOT pick any Execution for Group ${forGroup}")
        }

        return result
    }
}


object PickTasksBasedOnCustomQuery {

    @JvmStatic
    fun main(args: Array<String>) {
        val workerId = WorkerId("UnluckyInternDoingManualWork")
        val coll = emptyLocalCollection("sample08tasks")
        val dao = CustomQueryTasksDao(coll)


        dao.submitTask("A", "group1")
        dao.submitTask("B", "group4")
        dao.submitTask("C", "group4")
        dao.submitTask("D", "group1")


        dao.pickNextTaskExecution(workerId, "group1")
        dao.pickNextTaskExecution(workerId, "group1")
        dao.pickNextTaskExecution(workerId, "group1")

        dao.pickNextTaskExecution(workerId, "group4")
        dao.pickNextTaskExecution(workerId, "group4")
        dao.pickNextTaskExecution(workerId, "group4")
    }
}