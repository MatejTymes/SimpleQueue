package mtymes.common.time

import mtymes.common.time.DateUtil.UTC_ZONE_ID
import java.time.ZonedDateTime

class Clock {

    fun now(): ZonedDateTime {
        return ZonedDateTime.now(UTC_ZONE_ID)
    }
}
