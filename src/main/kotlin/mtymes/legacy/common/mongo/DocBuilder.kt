package mtymes.legacy.common.mongo

import mtymes.legacy.common.time.DateUtil.toDate
import org.bson.Document
import java.time.ZonedDateTime
import java.util.*


class DocBuilder {

    companion object {

        fun docBuilder() = DocBuilder()

        fun emptyDoc() = docBuilder().build()

        fun doc(key: String, value: Any?): Document {
            return docBuilder().put(key, value).build()
        }

        fun doc(vararg pairs: Pair<String, Any?>): Document {
            return docBuilder().putAll(*pairs).build()
        }

        fun doc(map: Map<String, Any?>): Document {
            return docBuilder().putAll(map).build()
        }
    }

    private val values = linkedMapOf<String, Any?>()

    fun build(): Document {
        return Document(values)
    }

    fun put(key: String, value: Any?): DocBuilder {
        if (value is Optional<*>) {
            if (value.isPresent) {
                values.put(key, mapValue(value.get()))
            }
        } else {
            values.put(key, mapValue(value))
        }
        return this
    }

    fun put(keyValue: Pair<String, Any?>): DocBuilder {
        return put(keyValue.first, keyValue.second)
    }

    fun putAll(vararg pairs: Pair<String, Any?>): DocBuilder {
        for ((key, value) in pairs) {
            put(key, value)
        }
        return this
    }

    fun putAll(values: Map<String, Any?>): DocBuilder {
        for (entry in values.entries) {
            put(entry.key, entry.value)
        }
        return this
    }

    private fun mapValue(value: Any?): Any? {
        var valueToUse = value

        if (valueToUse is UUID) {
            valueToUse = valueToUse.toString()
        } else if (valueToUse is Enum<*>) {
            valueToUse = valueToUse.name
        } else if (valueToUse is ZonedDateTime) {
            valueToUse = toDate(valueToUse)
        } else if (valueToUse is Collection<*>) {
            valueToUse = valueToUse
                    .filter { v -> if (v is Optional<*>) v.isPresent else true }
                    .map { v -> mapValue(v) }
                    .toList()
        } else if (valueToUse is Map<*, *>) {
            val map = valueToUse as Map<String, Any?>
            valueToUse = doc(map)
        }

        return valueToUse
    }
}