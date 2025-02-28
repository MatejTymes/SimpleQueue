package mtymes.tasks.worker.sweatshop

import mtymes.tasks.common.domain.WorkerId
import mtymes.tasks.worker.Worker
import mtymes.tasks.worker.sweatshop.ShutDownMode.Immediately
import java.io.Closeable

data class WorkerSummary(
    val workerId: WorkerId,
    val worker: Worker<out Any?>,
    val isWorking: Boolean,
    val hasNeverEndingStreamOfWork: Boolean,
    val whenShouldStop: ShutDownMode?
)

enum class ShutDownMode(
    val priority: Int,
    val isGraceful: Boolean
) {
    Immediately(3, false),
    OnceCurrentWorkIsFinished(2, true),
    OnceNoMoreWork(1, true) {
        override fun modeToUseIf(hasNeverEndingStreamOfWork: Boolean): ShutDownMode {
            return if (hasNeverEndingStreamOfWork) {
                OnceCurrentWorkIsFinished
            } else {
                this
            }
        }
    };

    open fun modeToUseIf(hasNeverEndingStreamOfWork: Boolean): ShutDownMode {
        return this
    }
}

enum class UpdateOutcome{
    WasApplied,
    WasNotApplied,
    WasAlreadyInWantedState
}


// todo: mtymes - add the ability for heart beat thread to interrupt the work thread (if for example the execution has been cancelled/is dead)
// todo: mtymes - implement other alternatives (shared threads or coroutines) and compare performance/stability
interface SweatShop : Closeable {

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