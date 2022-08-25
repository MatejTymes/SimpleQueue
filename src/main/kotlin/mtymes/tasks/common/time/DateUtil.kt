package mtymes.tasks.common.time

import java.time.*
import java.util.*

object DateUtil {

    val UTC_ZONE_ID = ZoneId.of("UTC")

    fun toDate(dateTime: ZonedDateTime): Date {
        return Date.from(dateTime.withZoneSameInstant(UTC_ZONE_ID).toInstant())
    }

    fun toDate(dateTime: LocalDateTime): Date {
        return toDate(dateTime.atZone(UTC_ZONE_ID))
    }

    fun toDate(localDate: LocalDate): Date {
        return toDate(localDate.atTime(LocalTime.MIDNIGHT));
    }

    fun toZonedDateTime(date: Date, zoneId: ZoneId): ZonedDateTime {
        return ZonedDateTime.ofInstant(
                date.toInstant(),
                zoneId
        )
    }

}