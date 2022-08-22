package mtymes.task.v02.worker

import mtymes.task.v02.scheduler.domain.WorkerId
import java.time.Duration


interface HeartBeatingTaskWorker<Task> : TaskWorker<Task> {

    fun heartBeatInterval(
        task: Task,
        workerId: WorkerId
    ): Duration

    fun updateHeartBeat(
        task: Task,
        workerId: WorkerId
    )
}