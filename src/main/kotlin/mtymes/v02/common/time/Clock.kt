package mtymes.v02.common.time

import mtymes.common.time.DateUtil.UTC_ZONE_ID
import java.time.ZonedDateTime


interface Clock {

    fun now(): ZonedDateTime
}


class UTCClock : Clock {

    override fun now(): ZonedDateTime {
        return ZonedDateTime.now(UTC_ZONE_ID)
    }
}
