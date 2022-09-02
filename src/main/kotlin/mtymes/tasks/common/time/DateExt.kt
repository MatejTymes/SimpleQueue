package mtymes.tasks.common.time

import java.time.ZonedDateTime
import java.util.*

object DateExt {

    fun Date.toUTCDateTime(): ZonedDateTime {
        return DateUtil.toUTCDateTime(this)
    }
}