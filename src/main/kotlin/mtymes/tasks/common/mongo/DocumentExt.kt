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

    fun Document.getUTCDateTime(key: String): ZonedDateTime {
        return this.getDate(key).toUTCDateTime()
    }

    fun Document.getNullableUTCDateTime(key: String): ZonedDateTime? {
        return this.getDate(key)?.toUTCDateTime()
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