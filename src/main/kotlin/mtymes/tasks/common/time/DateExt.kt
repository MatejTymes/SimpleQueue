package mtymes.tasks.common.time

import mtymes.tasks.common.time.DateUtil.UTC_ZONE_ID
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.ZonedDateTime
import java.util.*

object DateExt {

    fun Date.toZonedDateTime(zoneId: ZoneId = UTC_ZONE_ID): ZonedDateTime {
        return DateUtil.toZonedDateTime(this, zoneId)
    }

    fun Date.toLocalDateTime(zoneId: ZoneId = UTC_ZONE_ID): LocalDateTime {
        return DateUtil.toLocalDateTime(this, zoneId)
    }

    fun Date.toLocalDate(zoneId: ZoneId = UTC_ZONE_ID): LocalDate {
        return DateUtil.toLocalDate(this, zoneId)
    }
}