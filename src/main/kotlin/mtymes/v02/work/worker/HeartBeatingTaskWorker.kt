package mtymes.v02.work.worker

import mtymes.v02.scheduler.domain.WorkerId
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