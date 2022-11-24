package mtymes.tasks.common.time

import java.time.Duration

object DurationExt {

    fun Int.millisecond() : Duration = Duration.ofMillis(this.toLong())
    fun Int.milliseconds() : Duration = Duration.ofMillis(this.toLong())
    fun Int.second(): Duration = Duration.ofSeconds(this.toLong())
    fun Int.seconds(): Duration = Duration.ofSeconds(this.toLong())
    fun Int.minute(): Duration = Duration.ofMinutes(this.toLong())
    fun Int.minutes(): Duration = Duration.ofMinutes(this.toLong())
    fun Int.hour(): Duration = Duration.ofHours(this.toLong())
    fun Int.hours(): Duration = Duration.ofHours(this.toLong())
    fun Int.day(): Duration = Duration.ofDays(this.toLong())
    fun Int.days(): Duration = Duration.ofDays(this.toLong())
    fun Int.week(): Duration = Duration.ofDays(this.toLong() * 7)
    fun Int.weeks(): Duration = Duration.ofDays(this.toLong() * 7)
    fun Int.year(): Duration = Duration.ofDays(this.toLong() * 365)
    fun Int.years(): Duration = Duration.ofDays(this.toLong() * 365)

//    fun Duration.toHumanReadableString() {
//        val numberOfDays = this.toDays()
//        val numberOfYears = numberOfDays / 365
//    }
}