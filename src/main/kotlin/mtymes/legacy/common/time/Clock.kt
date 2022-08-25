package mtymes.legacy.common.time

import mtymes.legacy.common.time.DateUtil.UTC_ZONE_ID
import java.time.ZonedDateTime

class Clock {

    fun now(): ZonedDateTime {
        return ZonedDateTime.now(UTC_ZONE_ID)
    }
}
