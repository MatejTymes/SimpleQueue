package mtymes.task.v02.worker.sweatshop

import mtymes.task.v02.scheduler.domain.WorkerId
import mtymes.task.v02.worker.Worker
import java.io.Closeable

data class WorkerSummary(
    val workerId: WorkerId,
    val worker: Worker<out Any?>,
    val isWorking: Boolean,
    val isGracefullyDying: Boolean
)

interface SweatShop : AutoCloseable, Closeable {

    fun <T> addAndStartWorker(
        worker: Worker<T>,
        workerId: WorkerId = WorkerId.uniqueWorkerId()
    ): WorkerId

    fun stopAndRemoveWorker(
        workerId: WorkerId,
        stopGracefully: Boolean
    ): Boolean

    fun workerSummaries(): List<WorkerSummary>
}