package mtymes.tasks.worker.domain

import java.util.Collections.unmodifiableList
import java.util.concurrent.CopyOnWriteArrayList

data class InterruptableWork<Work, Interruption>(
    val work: Work
) {
    private val interruptions = CopyOnWriteArrayList<Interruption>()

    fun addInterruption(interruption: Interruption) {
        interruptions.add(interruption)
    }

    fun shouldBeInterrupted(): Boolean {
        return interruptions.isNotEmpty()
    }

    fun getInterruptions(): List<Interruption> {
        return unmodifiableList(interruptions)
    }
}