package mtymes.common.time

import java.time.ZoneId
import java.time.ZonedDateTime
import java.util.*

object DateUtil {

    val UTC_ZONE_ID = ZoneId.of("UTC")

    fun toDate(dateTime: ZonedDateTime): Date {
        return Date.from(dateTime.withZoneSameInstant(UTC_ZONE_ID).toInstant())
    }

    fun toZonedDateTime(date: Date, zoneId: ZoneId): ZonedDateTime {
        return ZonedDateTime.ofInstant(
                date.toInstant(),
                zoneId
        )
    }

}