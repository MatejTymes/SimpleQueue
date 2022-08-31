package mtymes.tasks.common.time

import java.time.Duration

object Durations {

    val ZERO_SECONDS: Duration = Duration.ofSeconds(0)

    val ONE_MILLISECOND: Duration = Duration.ofMillis(1)
    val TEN_MILLISECOND: Duration = Duration.ofMillis(10)
    val HUNDRED_MILLISECOND: Duration = Duration.ofMillis(100)

    val ONE_SECOND: Duration = Duration.ofSeconds(1)
    val TWO_SECONDS: Duration = Duration.ofSeconds(2)
    val THREE_SECONDS: Duration = Duration.ofSeconds(3)
    val FOUR_SECONDS: Duration = Duration.ofSeconds(4)
    val FIVE_SECONDS: Duration = Duration.ofSeconds(5)
    val TEN_SECONDS: Duration = Duration.ofSeconds(10)
    val THIRTY_SECONDS: Duration = Duration.ofSeconds(30)

    val ONE_MINUTE: Duration = Duration.ofMinutes(1)
    val TWO_MINUTES: Duration = Duration.ofMinutes(2)
    val THREE_MINUTES: Duration = Duration.ofMinutes(3)
    val FOUR_MINUTES: Duration = Duration.ofMinutes(4)
    val FIVE_MINUTES: Duration = Duration.ofMinutes(5)
    val TEN_MINUTES: Duration = Duration.ofMinutes(10)
    val THIRTY_MINUTES: Duration = Duration.ofMinutes(30)

    val ONE_HOUR: Duration = Duration.ofHours(1)
    val TWO_HOURS: Duration = Duration.ofHours(2)
    val THREE_HOURS: Duration = Duration.ofHours(3)
    val FOUR_HOURS: Duration = Duration.ofHours(4)
    val FIVE_HOURS: Duration = Duration.ofHours(5)
    val SIX_HOURS: Duration = Duration.ofHours(6)
    val SEVEN_HOURS: Duration = Duration.ofHours(7)
    val EIGHT_HOURS: Duration = Duration.ofHours(8)
    val NINE_HOURS: Duration = Duration.ofHours(9)
    val TEN_HOURS: Duration = Duration.ofHours(10)
    val ELEVEN_HOURS: Duration = Duration.ofHours(11)
    val TWELVE_HOURS: Duration = Duration.ofHours(12)
    val TWENTY_FOUR_HOURS: Duration = Duration.ofHours(24)

    val ONE_DAY: Duration = Duration.ofDays(1)
    val TWO_DAYS: Duration = Duration.ofDays(2)
    val THREE_DAYS: Duration = Duration.ofDays(3)
    val FOUR_DAYS: Duration = Duration.ofDays(4)
    val FIVE_DAYS: Duration = Duration.ofDays(5)
    val SIX_DAYS: Duration = Duration.ofDays(6)
    val SEVEN_DAYS: Duration = Duration.ofDays(7)
    val TEN_DAYS: Duration = Duration.ofDays(10)

    val ONE_WEEK = SEVEN_DAYS
    val TWO_WEEKS = Duration.ofDays(14)
    val THREE_WEEKS = Duration.ofDays(21)
    val FOUR_WEEKS = Duration.ofDays(28)
}