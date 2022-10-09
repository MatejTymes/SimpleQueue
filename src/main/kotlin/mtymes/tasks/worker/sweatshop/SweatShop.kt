package mtymes.tasks.worker.sweatshop

import mtymes.tasks.common.domain.WorkerId
import mtymes.tasks.worker.Worker
import mtymes.tasks.worker.sweatshop.ShutDownMode.Immediately
import java.io.Closeable

data class WorkerSummary(
    val workerId: WorkerId,
    val worker: Worker<out Any?>,
    val isWorking: Boolean,
    val whenShouldStop: ShutDownMode?
)

enum class ShutDownMode(
    val priority: Int,
    val isGraceful: Boolean
) {
    Immediately(3, false),
    OnceCurrentTaskIsFinished(2, true),
    OnceNoMoreWork(1, true)
}

enum class UpdateOutcome{
    WasApplied,
    WasNotApplied,
    WasAlreadyInWantedState
}


// todo: mtymes - add the ability for heart beat thread to interrupt the work thread (if for example the execution has been cancelled/has died)
// todo: mtymes - implement other alternatives (shared threads or coroutines) and compare performance/stability
interface SweatShop : AutoCloseable, Closeable {

    fun <T> addAndStartWorker(
        worker: Worker<T>,
        workerId: WorkerId = WorkerId.uniqueWorkerId()
    ): WorkerId

    fun stopWorker(
        workerId: WorkerId,
        shutDownMode: ShutDownMode = Immediately
    ): UpdateOutcome

    fun workerSummaries(): List<WorkerSummary>

    fun stopAllWorkers(
        shutDownMode: ShutDownMode,
        waitTillDone: Boolean
    )

    fun close(
        shutDownMode: ShutDownMode,
        waitTillDone: Boolean = false
    )

    override fun close() {
        close(
            shutDownMode = Immediately,
            waitTillDone = false
        )
    }
}