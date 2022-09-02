package mtymes.tasks.samples.sample09

import com.mongodb.client.MongoCollection
import mtymes.tasks.common.domain.WorkerId
import mtymes.tasks.common.mongo.DocBuilder.Companion.doc
import mtymes.tasks.common.time.Durations.FIVE_MINUTES
import mtymes.tasks.common.time.Durations.SEVEN_DAYS
import mtymes.tasks.common.time.Durations.TWO_HOURS
import mtymes.tasks.common.time.Durations.TWO_MINUTES
import mtymes.tasks.scheduler.dao.GenericScheduler
import mtymes.tasks.scheduler.dao.SchedulerDefaults
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.DATA
import mtymes.tasks.scheduler.domain.*
import mtymes.tasks.test.mongo.emptyLocalCollection
import mtymes.tasks.test.task.TaskViewer
import org.bson.Document
import printTimedString


data class TaskToProcess(
    val taskId: TaskId,
    val executionId: ExecutionId,
    val request: String,
    val group: String
)


class CancellationSupportingTaskDao(
    tasksCollection: MongoCollection<Document>
) {
    val scheduler = GenericScheduler(
        collection = tasksCollection,
        defaults = SchedulerDefaults(

            submitTaskOptions = SubmitTaskOptions(
                ttl = TWO_HOURS,
                maxAttemptsCount = 3,
                submitAsPaused = true
            ),

            fetchNextExecutionOptions = FetchNextExecutionOptions(
                keepAliveFor = FIVE_MINUTES
            ),

            markTasksAsCancelledOptions = MarkTasksAsCancelledOptions(
                newTTL = TWO_MINUTES
            ),

            markTasksAsUnPausedOptions = MarkTasksAsUnPausedOptions(
                newTTL = SEVEN_DAYS
            )
        )
    )

    fun submitPausedTask(
        request: String,
        group: String,
    ): TaskId? {
        val taskId = scheduler.submitTask(
            customData = doc(
                "request" to request,
                "group" to group
            )
        )

        printTimedString("submitted paused Task '${request}/${group}'")

        return taskId
    }

    fun fetchNextTaskExecution(
        workerId: WorkerId
    ): TaskToProcess? {
        val result = scheduler.fetchNextAvailableExecution(workerId)
            ?.let { summary ->
                val taskData = summary.underlyingTask.data()

                TaskToProcess(
                    taskId = summary.underlyingTask.taskId,
                    executionId = summary.fetchedExecution.executionId,
                    request = taskData.getString("request"),
                    group = taskData.getString("group")
                )
            }

        if (result != null) {
            printTimedString("fetched Execution ${result.executionId} - ${result.request}/${result.group}")
        } else {
            printTimedString("did NOT fetch any Execution")
        }

        return result
    }

    fun markTaskAsCancelled(
        taskId: TaskId,
        cancellationReason: String
    ) {
        val result = scheduler.markTaskAsCancelled(
            taskId = taskId,
            additionalTaskData = doc(
                "cancellationReason" to cancellationReason
            )
        )
        if (result != null) {
            printTimedString("marked Task ${taskId} as CANCELLED")
        } else {
            printTimedString("did NOT mark Task ${taskId} as CANCELLED")
        }
    }

    fun cancelTasks(
        group: String
    ) {
        val cancelledTasksCount: Long = scheduler.markTasksAsCancelled(
            doc(DATA + ".group" to group)
        )

        if (cancelledTasksCount == 0L) {
            printTimedString("cancelled NO Tasks for group ${group}")
        } else {
            printTimedString("cancelled ${cancelledTasksCount} Tasks for group ${group}")
        }
    }

    fun unPauseTasks(
        group: String
    ) {
        val cancelledTasksCount: Long = scheduler.markTasksAsUnPaused(
            doc(DATA + ".group" to group)
        )

        if (cancelledTasksCount == 0L) {
            printTimedString("unPaused NO Tasks for group ${group}")
        } else {
            printTimedString("unPaused ${cancelledTasksCount} Tasks for group ${group}")
        }
    }
}


object SubmitPausedTasksAsAGroup {

    @JvmStatic
    fun main(args: Array<String>) {
        val workerId = WorkerId("UnluckyInternDoingManualWork")
        val coll = emptyLocalCollection("sample09tasks")
        val dao = CancellationSupportingTaskDao(coll)


        dao.submitPausedTask("A", "group1")
        dao.submitPausedTask("B", "group2")
        dao.submitPausedTask("C", "group1")
        dao.submitPausedTask("D", "group1")
        dao.submitPausedTask("E", "group2")

        dao.fetchNextTaskExecution(workerId) // fetches nothing


        dao.cancelTasks("group1")

        dao.fetchNextTaskExecution(workerId) // fetches nothing


        dao.unPauseTasks("group2")

        dao.fetchNextTaskExecution(workerId) // fetches B/group2
        dao.fetchNextTaskExecution(workerId) // fetches E/group2

        dao.fetchNextTaskExecution(workerId) // fetches nothing


        TaskViewer.displayTinyTasksSummary(
            coll,
            setOf("deletableAfter")
        )
    }
}
