package mtymes.tasks.worker

import mtymes.tasks.common.domain.WorkerId
import java.time.Duration


interface HeartBeatingWorker<Task> : Worker<Task> {

    fun heartBeatInterval(
        task: Task,
        workerId: WorkerId
    ): Duration

    fun updateHeartBeat(
        task: Task,
        workerId: WorkerId
    )
}