package mtymes.v02.work.worker

import mtymes.v02.scheduler.domain.WorkerId
import java.time.Duration


interface TaskWorker<Task> {

    fun pickNextTask(
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
    )

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
            1L -> Duration.ofSeconds(1)
            2L -> Duration.ofSeconds(3)
            3L -> Duration.ofSeconds(10)
            4L -> Duration.ofSeconds(30)
            else -> Duration.ofSeconds(60)
        }
    }
}