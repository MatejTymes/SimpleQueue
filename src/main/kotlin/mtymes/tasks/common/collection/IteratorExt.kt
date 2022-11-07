package mtymes.tasks.common.collection

object IteratorExt {

    fun <T, R> Iterator<T>.map(transform: (T) -> R): Iterator<R> {
        return MappingIterator(this, transform)
    }


    class MappingIterator<I, O>(
        private val sourceIterator: Iterator<I>,
        private val transform: (I) -> O
    ) : Iterator<O> {

        override fun hasNext(): Boolean {
            return sourceIterator.hasNext()
        }

        override fun next(): O {
            return transform.invoke(sourceIterator.next())
        }
    }
}