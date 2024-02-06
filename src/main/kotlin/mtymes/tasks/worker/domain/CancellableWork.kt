package mtymes.tasks.worker.domain

import java.util.concurrent.atomic.AtomicBoolean

data class CancellableWork<Work>(
     val work: Work
) {
    private val shouldBeCancelled = AtomicBoolean(false)

    fun notifyShouldBeCancelled() {
        shouldBeCancelled.set(true)
    }

    fun shouldBeCancelled(): Boolean {
        return shouldBeCancelled.get()
    }
}