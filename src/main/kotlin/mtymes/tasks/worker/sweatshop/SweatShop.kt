package mtymes.tasks.worker.sweatshop

import mtymes.tasks.common.domain.WorkerId
import mtymes.tasks.worker.Worker
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

    fun closeGracefully(
        waitTillDone: Boolean = false
    )
}