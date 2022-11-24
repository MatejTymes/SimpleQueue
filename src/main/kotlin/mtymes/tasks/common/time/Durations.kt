package mtymes.tasks.common.time

import mtymes.tasks.common.time.DurationExt.day
import mtymes.tasks.common.time.DurationExt.days
import mtymes.tasks.common.time.DurationExt.hour
import mtymes.tasks.common.time.DurationExt.hours
import mtymes.tasks.common.time.DurationExt.millisecond
import mtymes.tasks.common.time.DurationExt.milliseconds
import mtymes.tasks.common.time.DurationExt.minute
import mtymes.tasks.common.time.DurationExt.minutes
import mtymes.tasks.common.time.DurationExt.second
import mtymes.tasks.common.time.DurationExt.seconds
import mtymes.tasks.common.time.DurationExt.weeks
import java.time.Duration

object Durations {

    val ZERO_SECONDS: Duration = 0.seconds()

    val ONE_MILLISECOND: Duration = 1.millisecond()
    val TEN_MILLISECOND: Duration = 10.milliseconds()
    val HUNDRED_MILLISECOND: Duration = 100.milliseconds()

    val ONE_SECOND: Duration = 1.second()
    val TWO_SECONDS: Duration = 2.seconds()
    val THREE_SECONDS: Duration = 3.seconds()
    val FOUR_SECONDS: Duration = 4.seconds()
    val FIVE_SECONDS: Duration = 5.seconds()
    val TEN_SECONDS: Duration = 10.seconds()
    val FIFTEEN_SECONDS: Duration = 15.seconds()
    val TWENTY_SECONDS: Duration = 20.seconds()
    val THIRTY_SECONDS: Duration = 30.seconds()
    val FOURTY_SECONDS: Duration = 40.seconds()
    val FOURTY_FIVE_SECONDS: Duration = 45.seconds()
    val FIFTY_SECONDS: Duration = 50.seconds()

    val ONE_MINUTE: Duration = 1.minute()
    val TWO_MINUTES: Duration = 2.minutes()
    val THREE_MINUTES: Duration = 3.minutes()
    val FOUR_MINUTES: Duration = 4.minutes()
    val FIVE_MINUTES: Duration = 5.minutes()
    val TEN_MINUTES: Duration = 10.minutes()
    val FIFTEEN_MINUTES: Duration = 15.minutes()
    val TWENTY_MINUTES: Duration = 20.minutes()
    val THIRTY_MINUTES: Duration = 30.minutes()
    val FOURTY_MINUTES: Duration = 40.minutes()
    val FOURTY_FIVE_MINUTES: Duration = 45.minutes()
    val FIFTY_MINUTES: Duration = 50.minutes()

    val ONE_HOUR: Duration = 1.hour()
    val TWO_HOURS: Duration = 2.hours()
    val THREE_HOURS: Duration = 3.hours()
    val FOUR_HOURS: Duration = 4.hours()
    val FIVE_HOURS: Duration = 5.hours()
    val SIX_HOURS: Duration = 6.hours()
    val SEVEN_HOURS: Duration = 7.hours()
    val EIGHT_HOURS: Duration = 8.hours()
    val NINE_HOURS: Duration = 9.hours()
    val TEN_HOURS: Duration = 10.hours()
    val ELEVEN_HOURS: Duration = 11.hours()
    val TWELVE_HOURS: Duration = 12.hours()
    val EIGHTEEN_HOURS: Duration = 18.hours()
    val TWENTY_FOUR_HOURS: Duration = 24.hours()

    val ONE_DAY: Duration = 1.day()
    val TWO_DAYS: Duration = 2.days()
    val THREE_DAYS: Duration = 3.days()
    val FOUR_DAYS: Duration = 4.days()
    val FIVE_DAYS: Duration = 5.days()
    val SIX_DAYS: Duration = 6.days()
    val SEVEN_DAYS: Duration = 7.days()
    val TEN_DAYS: Duration = 10.days()

    val ONE_WEEK = SEVEN_DAYS
    val TWO_WEEKS = 2.weeks()
    val THREE_WEEKS = 3.weeks()
    val FOUR_WEEKS = 4.weeks()
}