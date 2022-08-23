package mtymes.task.v02.worker.sweatshop

import mtymes.task.v02.scheduler.domain.WorkerId
import mtymes.task.v02.worker.Worker
import java.io.Closeable

interface SweatShop : AutoCloseable, Closeable {

    fun <T> addAndStartWorker(
        worker: Worker<T>,
        workerId: WorkerId = WorkerId.uniqueWorkerId()
    ): WorkerId

    fun stopAndRemoveWorker(
        workerId: WorkerId,
        stopGracefully: Boolean
    ): Boolean

    fun registeredWorkers(): Map<WorkerId, Worker<out Any?>>
}