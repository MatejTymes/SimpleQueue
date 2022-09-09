package mtymes.tasks.common.mongo

import mtymes.tasks.common.time.DateExt.toLocalDate
import mtymes.tasks.common.time.DateExt.toLocalDateTime
import mtymes.tasks.common.time.DateExt.toZonedDateTime
import mtymes.tasks.common.time.DateUtil.UTC_ZONE_ID
import org.bson.Document
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.ZonedDateTime

object DocumentExt {

    fun Document?.isDefined(): Boolean {
        return !this.isNullOrEmpty()
    }

    fun Document?.areDefined(): Boolean {
        return !this.isNullOrEmpty()
    }

    fun Document.getZonedDateTime(key: String, zoneId: ZoneId = UTC_ZONE_ID): ZonedDateTime {
        return this.getDate(key).toZonedDateTime(zoneId)
    }

    fun Document.getNullableZonedDateTime(key: String, zoneId: ZoneId = UTC_ZONE_ID): ZonedDateTime? {
        return this.getDate(key)?.toZonedDateTime(zoneId)
    }

    fun Document.getLocalDateTime(key: String, zoneId: ZoneId = UTC_ZONE_ID): LocalDateTime {
        return this.getDate(key).toLocalDateTime(zoneId)
    }

    fun Document.getLocalDate(key: String, zoneId: ZoneId = UTC_ZONE_ID): LocalDate {
        return this.getDate(key).toLocalDate(zoneId)
    }

    fun Document.getDocument(key: String): Document {
        return this.get(key) as Document
    }

    fun Document.getNullableDocument(key: String): Document? {
        return this.get(key) as Document?
    }

    fun Document.getListOfDocuments(key: String): List<Document> {
        return this.getList(key, Document::class.java)
    }
}