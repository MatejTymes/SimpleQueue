package mtymes.tasks.worker

import mtymes.tasks.common.domain.WorkerId
import mtymes.tasks.common.time.Durations.ONE_MILLISECOND
import mtymes.tasks.common.time.Durations.ONE_MINUTE
import mtymes.tasks.common.time.Durations.ONE_SECOND
import mtymes.tasks.common.time.Durations.TEN_SECONDS
import mtymes.tasks.common.time.Durations.THIRTY_SECONDS
import mtymes.tasks.common.time.Durations.THREE_SECONDS
import java.io.Closeable
import java.io.IOException
import java.time.Duration


interface Worker<Work> : Closeable {

    fun pickAvailableWork(
        workerId: WorkerId
    ): Work?

    fun processWork(
        work: Work,
        workerId: WorkerId
    )

    fun handleWorkFailure(
        work: Work,
        workerId: WorkerId,
        exception: Exception
    ) {
        // do nothing
    }

    fun workerLogId(workerId: WorkerId): String {
        return "${ this::class.simpleName?.let{ it + ":" } ?: "" }${ workerId }"
    }

    fun workToLoggableString(
        work: Work,
        workerId: WorkerId
    ): String {
        return work.toString()
    }

    fun sleepDurationIfNoWorkWasAvailable(
        workNotFoundNTimesInARow: Long,
        workerId: WorkerId
    ): Duration {
        return when(workNotFoundNTimesInARow) {
            1L -> ONE_SECOND
            2L -> THREE_SECONDS
            3L -> TEN_SECONDS
            4L -> THIRTY_SECONDS
            else -> ONE_MINUTE
        }
    }

    fun sleepDurationIfWorkWasProcessed(
        workerId: WorkerId
    ): Duration {
        return ONE_MILLISECOND
    }

    fun hasNeverEndingStreamOfWork(): Boolean = false

    @Throws(IOException::class)
    override fun close() {
        // do nothing
    }
}
