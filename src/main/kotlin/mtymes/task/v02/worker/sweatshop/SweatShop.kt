package mtymes.task.v02.worker.sweatshop

import mtymes.task.v02.scheduler.domain.WorkerId
import mtymes.task.v02.worker.TaskWorker
import java.io.Closeable

interface SweatShop : AutoCloseable, Closeable {

    fun <T> addAndStartWorker(
        worker: TaskWorker<T>,
        workerId: WorkerId = WorkerId.uniqueWorkerId()
    )

    fun stopAndRemoveWorker(
        workerId: WorkerId,
        stopGracefully: Boolean
    ): Boolean
}