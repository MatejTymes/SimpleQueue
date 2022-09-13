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

// todo: mtymes - add the ability for heart beat thread to interrupt the work thread (if for example the execution has been cancelled/timedOut)
// todo: mtymes - implement other alternatives (shared threads or coroutines) and compare performance/stability
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

    // todo: mtymes - add option to only close a worker once it fails to acquire some work
    fun closeGracefully(
        waitTillDone: Boolean = false
    )
}