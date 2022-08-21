package mtymes.v02.work.sweatshop

import mtymes.v02.scheduler.domain.WorkerId
import mtymes.v02.work.worker.TaskWorker

interface SweatShop : AutoCloseable {

    fun <T> addAndStartWorker(
        worker: TaskWorker<T>,
        workerId: WorkerId = WorkerId.uniqueWorkerId()
    )

    fun stopAndRemoveWorker(
        workerId: WorkerId,
        stopGracefully: Boolean
    ): Boolean
}