package mtymes.tasks.worker

import mtymes.tasks.common.domain.WorkerId
import java.time.Duration


interface HeartBeatingWorker<Work> : Worker<Work> {

    fun heartBeatInterval(
        work: Work,
        workerId: WorkerId
    ): Duration

    fun updateHeartBeat(
        work: Work,
        workerId: WorkerId
    )
}