package mtymes.tasks.common.time

import java.time.*
import java.util.*

object DateUtil {

    val UTC_ZONE_ID = ZoneId.of("UTC")

    fun toDate(
        dateTime: ZonedDateTime,
        zoneId: ZoneId = UTC_ZONE_ID
    ): Date {
        return Date.from(dateTime.withZoneSameInstant(zoneId).toInstant())
    }

    fun toDate(
        dateTime: LocalDateTime,
        zoneId: ZoneId = UTC_ZONE_ID
    ): Date {
        return toDate(dateTime.atZone(zoneId))
    }

    fun toDate(
        localDate: LocalDate,
        zoneId: ZoneId = UTC_ZONE_ID
    ): Date {
        return toDate(localDate.atTime(LocalTime.MIDNIGHT), zoneId)
    }

    fun toZonedDateTime(
        date: Date,
        zoneId: ZoneId = UTC_ZONE_ID
    ): ZonedDateTime {
        return ZonedDateTime.ofInstant(date.toInstant(), zoneId)
    }

    fun toLocalDateTime(
        date: Date,
        zoneId: ZoneId = UTC_ZONE_ID
    ): LocalDateTime {
        return LocalDateTime.ofInstant(date.toInstant(), zoneId)
    }

    fun toLocalDate(
        date: Date,
        zoneId: ZoneId = UTC_ZONE_ID
    ): LocalDate {
        return toLocalDateTime(date, zoneId).toLocalDate()
    }
}