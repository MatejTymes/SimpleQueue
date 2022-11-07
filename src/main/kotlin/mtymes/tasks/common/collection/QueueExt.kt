package mtymes.tasks.common.collection

import java.util.*

object QueueExt {

    fun <T> Queue<T>.consumeAll(action: (T) -> Unit): Unit {
        var value: T
        do {
            value = this.poll()

            if (value != null) {
                action(value)
            }
        } while (value != null)
    }

    fun <T> Queue<T>.pollingIterator(): Iterator<T> {
        return PollingIterator(this)
    }


    class PollingIterator<T>(
        private val queue: Queue<T>
    ): Iterator<T> {

        override fun hasNext(): Boolean {
            return queue.isNotEmpty()
        }

        override fun next(): T {
            if(queue.isEmpty()) {
                throw NoSuchElementException("Has no more values")
            } else {
                return queue.poll()
            }
        }
    }
}