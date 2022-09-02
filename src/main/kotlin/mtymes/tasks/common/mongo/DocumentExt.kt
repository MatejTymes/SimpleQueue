package mtymes.tasks.common.mongo

import mtymes.tasks.common.time.DateExt.toUTCDateTime
import org.bson.Document
import java.time.ZonedDateTime

object DocumentExt {

    fun Document?.isDefined(): Boolean {
        return !this.isNullOrEmpty()
    }

    fun Document?.areDefined(): Boolean {
        return !this.isNullOrEmpty()
    }

    fun Document.toUTCDateTime(key: String): ZonedDateTime {
        return this.getDate(key).toUTCDateTime()
    }

    fun Document.toNullableUTCDateTime(key: String): ZonedDateTime? {
        return this.getDate(key)?.toUTCDateTime()
    }
}