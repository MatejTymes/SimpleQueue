package mtymes.tasks.common.time

import mtymes.tasks.common.time.DateUtil.UTC_ZONE_ID
import java.time.ZonedDateTime


interface Clock {

    fun now(): ZonedDateTime
}


object UTCClock : Clock {

    override fun now(): ZonedDateTime {
        return ZonedDateTime.now(UTC_ZONE_ID)
    }
}
