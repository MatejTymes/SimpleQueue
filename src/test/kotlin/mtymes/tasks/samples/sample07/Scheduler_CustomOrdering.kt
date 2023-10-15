package mtymes.tasks.samples.sample07

import com.mongodb.client.MongoCollection
import mtymes.tasks.common.domain.WorkerId
import mtymes.tasks.common.mongo.builder.WithBaseDocBuilder
import mtymes.tasks.common.time.Durations
import mtymes.tasks.scheduler.dao.GenericScheduler
import mtymes.tasks.scheduler.dao.SchedulerDefaults
import mtymes.tasks.scheduler.dao.UniversalScheduler
import mtymes.tasks.scheduler.domain.ExecutionId
import mtymes.tasks.scheduler.domain.PickNextExecutionOptions
import mtymes.tasks.scheduler.domain.SubmitTaskOptions
import mtymes.tasks.scheduler.domain.TaskId
import mtymes.tasks.test.mongo.emptyLocalCollection
import org.bson.Document
import printTimedString


data class TaskToProcess(
    val taskId: TaskId,
    val executionId: ExecutionId,
    val request: String,
    val priority: Int
)


class PriorityOrderedTasksDao(
    tasksCollection: MongoCollection<Document>
) : WithBaseDocBuilder {
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
        request: String,
        priority: Int
    ): TaskId {
        val taskId = scheduler.submitTask(
            doc(
                "request" to request,
                "priority" to priority
            )
        )
        printTimedString("submitted Task '${request}' with Priority ${priority}")

        return taskId
    }

    fun pickNextTaskExecution(
        workerId: WorkerId
    ): TaskToProcess? {
        val result = scheduler
            .pickNextAvailableExecution(
                workerId = workerId,
                customSortOrder = doc(
                    "data.priority" to -1,
                    UniversalScheduler.CAN_BE_EXECUTED_AS_OF to 1
                )
            )?.let { summary ->
                TaskToProcess(
                    taskId = summary.underlyingTask.taskId,
                    executionId = summary.pickedExecution.executionId,
                    request = summary.underlyingTask.data().getString("request"),
                    priority = summary.underlyingTask.data().getInteger("priority")
                )
            }

        if (result != null) {
            printTimedString("picked Execution ${result.executionId} [${result.request}] of Priority ${result.priority}")
        } else {
            printTimedString("did NOT pick any Execution")
        }

        return result
    }

    fun updatePriority(
        taskId: TaskId,
        newPriority: Int
    ) {
        val result = scheduler.updateTaskData(
            taskId = taskId,
            additionalTaskData = doc(
                "priority" to newPriority
            )
        )
        if (result != null) {
            printTimedString("updated Task ${taskId} Priority to ${newPriority}")
        } else {
            printTimedString("did NOT update Task Priority")
        }
    }

    fun markAsFailed(
        executionId: ExecutionId,
        errorMessage: String,
        newPriority: Int
    ) {
        val result = scheduler.markAsFailedButCanRetry(
            executionId = executionId,
            additionalExecutionData = doc(
                "errorMessage" to errorMessage
            ),
            additionalTaskData = doc(
                "priority" to newPriority
            )
        )
        if (result != null) {
            printTimedString("marked Execution ${executionId} as FAILED and changed Priority to ${newPriority}")
        } else {
            printTimedString("did NOT mark Execution ${executionId} as FAILED")
        }
    }
}


object PickTasksBasedOnPriority {

    @JvmStatic
    fun main(args: Array<String>) {
        val workerId = WorkerId("UnluckyInternDoingManualWork")
        val coll = emptyLocalCollection("sample07tasks")
        val dao = PriorityOrderedTasksDao(coll)

        dao.submitTask("A", 5)
        dao.submitTask("B", 100)
        dao.submitTask("C", 1)
        dao.submitTask("D", 5)
        dao.submitTask("E", 5)

        dao.pickNextTaskExecution(workerId)
        dao.pickNextTaskExecution(workerId)
        dao.pickNextTaskExecution(workerId)
        dao.pickNextTaskExecution(workerId)
        dao.pickNextTaskExecution(workerId)
        dao.pickNextTaskExecution(workerId)
    }
}


object ChangingPriorityOnTheGo {

    @JvmStatic
    fun main(args: Array<String>) {
        val workerId = WorkerId("UnluckyInternDoingManualWork")
        val coll = emptyLocalCollection("sample07tasks")
        val dao = PriorityOrderedTasksDao(coll)

        val taskIdA = dao.submitTask("A", 5)
        val taskIdB = dao.submitTask("B", 100)
        val taskIdC = dao.submitTask("C", 1)

        dao.pickNextTaskExecution(workerId)

        dao.updatePriority(taskIdC, 130)
        dao.pickNextTaskExecution(workerId)

        dao.pickNextTaskExecution(workerId)
    }
}


object JumpQueueOnFailure {

    @JvmStatic
    fun main(args: Array<String>) {
        val workerId = WorkerId("UnluckyInternDoingManualWork")
        val coll = emptyLocalCollection("sample07tasks")
        val dao = PriorityOrderedTasksDao(coll)

        dao.submitTask("A", 5)
        dao.submitTask("B", 5)

        val executionIdA = dao.pickNextTaskExecution(workerId)!!.executionId

        dao.submitTask("C", 100)

        dao.markAsFailed(executionIdA, "failed, but we need to replay this quickly", 999)

        dao.pickNextTaskExecution(workerId)
        dao.pickNextTaskExecution(workerId)
        dao.pickNextTaskExecution(workerId)
    }
}