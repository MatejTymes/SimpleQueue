package mtymes.tasks.samples.sample05

import com.mongodb.client.MongoCollection
import mtymes.tasks.common.domain.WorkerId
import mtymes.tasks.common.mongo.builder.WithCoreDocBuilder
import mtymes.tasks.common.time.Durations
import mtymes.tasks.scheduler.dao.GenericScheduler
import mtymes.tasks.scheduler.dao.SchedulerDefaults
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.CAN_BE_EXECUTED_AS_OF
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.EXECUTIONS_COUNT
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.EXECUTION_ATTEMPTS_LEFT
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.LAST_EXECUTION
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.SUSPENSION_COUNT
import mtymes.tasks.scheduler.domain.*
import mtymes.tasks.scheduler.domain.StatusToPick.OnlyAvailable
import mtymes.tasks.scheduler.domain.StatusToPick.SuspendedAndAvailable
import mtymes.tasks.test.mongo.emptyLocalCollection
import mtymes.tasks.test.task.TaskViewer.displayTinyTasksSummary
import org.bson.Document
import printTimedString
import java.time.Duration


data class TaskToProcess(
    val executionId: ExecutionId,
    val request: String
)


class SuspendingTaskDao(
    tasksCollection: MongoCollection<Document>
) : WithCoreDocBuilder {
    val scheduler = GenericScheduler(
        collection = tasksCollection,
        defaults = SchedulerDefaults(

            submitTaskOptions = SubmitTaskOptions(
                ttl = Durations.SEVEN_DAYS,
                maxAttemptsCount = 3
            ),

            pickNextExecutionOptions = PickNextExecutionOptions(
                keepAliveFor = Durations.FIVE_MINUTES,
                statusToPick = SuspendedAndAvailable
            ),

            markAsSuspendedOptions = MarkAsSuspendedOptions(
                suspendFor = Durations.ZERO_SECONDS
            )
        )
    )

    fun submitTask(
        request: String,
        delayStartBy: Duration = Duration.ofSeconds(0)
    ) {
        scheduler.submitTask(
            customData = doc("request" to request),
            options = scheduler.defaults.submitTaskOptions!!.copy(
                delayStartBy = delayStartBy
            )
        )

        printTimedString("submitted Task '${request}'")
    }

    fun pickNextTaskExecution(
        workerId: WorkerId,
        statusToPick: StatusToPick = scheduler.defaults.pickNextExecutionOptions!!.statusToPick
    ): TaskToProcess? {
        val result = scheduler.pickNextAvailableExecution(
            workerId = workerId,
            options = scheduler.defaults.pickNextExecutionOptions!!.copy(
                statusToPick = statusToPick
            )
        )?.let { summary ->
            TaskToProcess(
                executionId = summary.pickedExecution.executionId,
                request = summary.underlyingTask.data().getString("request")
            )
        }

        if (result != null) {
            printTimedString("picked Execution ${result.executionId}")
        } else {
            printTimedString("did NOT pick any Execution" + if(statusToPick == OnlyAvailable) " (picking only NON-SUSPENDED tasks)" else "")
        }

        return result
    }

    fun markAsSuspended(
        executionId: ExecutionId,
        suspendFor: Duration
    ) {
        val result = scheduler.markAsSuspended(
            executionId = executionId,
            options = scheduler.defaults.markAsSuspendedOptions!!.copy(
                suspendFor = suspendFor
            )
        )
        if (result != null) {
            printTimedString("marked Execution ${executionId} as SUSPENDED for ${suspendFor}")
        } else {
            printTimedString("did NOT mark Execution ${executionId} as SUSPENDED")
        }
    }

    fun markAsSucceeded(
        executionId: ExecutionId,
        message: String
    ) {
        val result = scheduler.markAsSucceeded(
            executionId = executionId,
            additionalExecutionData = doc(
                "successMessage" to message
            )
        )
        if (result != null) {
            printTimedString("marked Execution ${executionId} as SUCCEEDED")
        } else {
            printTimedString("did NOT mark Execution ${executionId} as SUCCEEDED")
        }
    }
}


object DelayedStart {

    @JvmStatic
    fun main(args: Array<String>) {
        val workerId = WorkerId("UnluckyInternDoingManualWork")
        val coll = emptyLocalCollection("sample05tasks")
        val dao = SuspendingTaskDao(coll)


        dao.submitTask(
            request = "A",
            delayStartBy = Duration.ofSeconds(1)
        )

        displayTinyTasksSummary(coll, setOf(
            EXECUTION_ATTEMPTS_LEFT,
            EXECUTIONS_COUNT,
            CAN_BE_EXECUTED_AS_OF
        ))

        dao.pickNextTaskExecution(workerId)


        Thread.sleep(1_100)


        dao.pickNextTaskExecution(workerId)

        displayTinyTasksSummary(coll, setOf(
            EXECUTION_ATTEMPTS_LEFT,
            EXECUTIONS_COUNT,
            CAN_BE_EXECUTED_AS_OF
        ))
    }
}


object TaskSuspension {

    @JvmStatic
    fun main(args: Array<String>) {
        val workerId = WorkerId("UnluckyInternDoingManualWork")
        val coll = emptyLocalCollection("sample05tasks")
        val dao = SuspendingTaskDao(coll)

        dao.submitTask(request = "A")


        val executionId = dao.pickNextTaskExecution(workerId)!!.executionId
        dao.markAsSuspended(
            executionId = executionId,
            suspendFor = Duration.ofSeconds(0)
        )

        displayTinyTasksSummary(coll, setOf(
            EXECUTION_ATTEMPTS_LEFT,
            EXECUTIONS_COUNT,
            CAN_BE_EXECUTED_AS_OF,
            LAST_EXECUTION + "." + SUSPENSION_COUNT
        ))

        // pick non-suspendable only
        dao.pickNextTaskExecution(
            workerId = workerId,
            statusToPick = OnlyAvailable
        )

        // pick suspendable as well
        val anotherExecutionId = dao.pickNextTaskExecution(
            workerId = workerId,
            statusToPick = SuspendedAndAvailable
        )!!.executionId

        displayTinyTasksSummary(coll, setOf(
            EXECUTION_ATTEMPTS_LEFT,
            EXECUTIONS_COUNT,
            CAN_BE_EXECUTED_AS_OF,
            LAST_EXECUTION + "." + SUSPENSION_COUNT
        ))

        dao.markAsSuspended(
            executionId = anotherExecutionId,
            suspendFor = Duration.ofSeconds(1)
        )

        displayTinyTasksSummary(coll, setOf(
            EXECUTION_ATTEMPTS_LEFT,
            EXECUTIONS_COUNT,
            CAN_BE_EXECUTED_AS_OF,
            LAST_EXECUTION + "." + SUSPENSION_COUNT
        ))
        dao.pickNextTaskExecution(
            workerId = workerId
        )


        Thread.sleep(1_100)


        dao.pickNextTaskExecution(
            workerId = workerId
        )
    }
}

