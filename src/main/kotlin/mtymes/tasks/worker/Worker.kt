package mtymes.tasks.worker

import mtymes.tasks.common.time.Durations.ONE_MILLISECOND
import mtymes.tasks.common.time.Durations.ONE_MINUTE
import mtymes.tasks.common.time.Durations.ONE_SECOND
import mtymes.tasks.common.time.Durations.TEN_SECONDS
import mtymes.tasks.common.time.Durations.THIRTY_SECONDS
import mtymes.tasks.common.time.Durations.THREE_SECONDS
import mtymes.tasks.scheduler.domain.WorkerId
import java.time.Duration


interface Worker<Task> {

    fun fetchNextTaskToProcess(
        workerId: WorkerId
    ): Task?

    fun executeTask(
        task: Task,
        workerId: WorkerId
    )

    fun handleExecutionFailure(
        task: Task,
        workerId: WorkerId,
        exception: Exception
    ) {
        // do nothing
    }

    fun taskToLoggableString(
        task: Task,
        workerId: WorkerId
    ): String {
        return task.toString()
    }

    fun sleepDurationIfNoTaskWasAvailable(
        taskNotFoundNTimesInARow: Long,
        workerId: WorkerId
    ): Duration {
        return when(taskNotFoundNTimesInARow) {
            1L -> ONE_SECOND
            2L -> THREE_SECONDS
            3L -> TEN_SECONDS
            4L -> THIRTY_SECONDS
            else -> ONE_MINUTE
        }
    }

    fun sleepDurationIfTaskWasProcessed(
        workerId: WorkerId
    ): Duration {
        return ONE_MILLISECOND
    }
}