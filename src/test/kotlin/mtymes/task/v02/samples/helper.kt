import mtymes.task.v02.common.time.UTCClock
import java.util.*

fun printTimedString(text: String) {
    val dateTimeString = Date.from(UTCClock().now().toInstant()).toString()
    val timeString = dateTimeString.split(" ")[3]
    println("\n[$timeString]: ${text}")
}
