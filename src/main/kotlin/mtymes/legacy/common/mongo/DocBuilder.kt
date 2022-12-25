package mtymes.legacy.common.mongo

import javafixes.`object`.Microtype
import mtymes.tasks.common.time.DateUtil.toDate
import org.bson.Document
import java.net.URI
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZonedDateTime
import java.util.*

class DocBuilder {

    companion object {

        fun docBuilder() = DocBuilder()

        fun docBuilder(vararg pairs: Pair<String, Any?>) = DocBuilder().putAll(*pairs)

        fun docBuilder(map: Map<String, Any?>) = DocBuilder().putAll(map)

        fun emptyDoc() = DocBuilder().build()

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
            value.ifPresent {
                values.put(key, mapValue(it))
            }
        } else {
            values.put(key, mapValue(value))
        }
        return this
    }

    fun put(keyValue: Pair<String, Any?>): DocBuilder {
        return put(keyValue.first, keyValue.second)
    }

    fun putIf(condition: Boolean, key: String, value: Any?): DocBuilder {
        if (condition) {
            put(key, value)
        }
        return this
    }

    fun putIf(condition: Boolean, keyValue: Pair<String, Any?>): DocBuilder {
        return putIf(condition, keyValue.first, keyValue.second)
    }

    fun putIf(condition: Boolean, keyValueGenerator: () -> Pair<String, Any?>): DocBuilder {
        if (condition) {
            val (key, value) = keyValueGenerator.invoke()
            put(key, value)
        }
        return this
    }

    fun putAll(vararg pairs: Pair<String, Any?>): DocBuilder {
        for ((key, value) in pairs) {
            put(key, value)
        }
        return this
    }

    fun putAll(values: Map<String, Any?>?): DocBuilder {
        if (values != null) {
            for ((key, value) in values) {
                put(key, value)
            }
        }
        return this
    }

    fun putAllIf(condition: Boolean, valuesGenerator: () -> Map<String, Any?>): DocBuilder {
        if (condition) {
            putAll(valuesGenerator.invoke())
        }
        return this
    }

    private fun mapValue(value: Any?): Any? {
        var valueToUse = value

        if (valueToUse is Microtype<*>) {
            valueToUse = valueToUse.value()
        }

        if (valueToUse is UUID) {
            valueToUse = valueToUse.toString()
        } else if (valueToUse is Enum<*>) {
            valueToUse = valueToUse.name
        } else if (valueToUse is ZonedDateTime) {
            valueToUse = toDate(valueToUse)
        } else if (valueToUse is LocalDateTime) {
            valueToUse = toDate(valueToUse)
        } else if (valueToUse is LocalDate) {
            valueToUse = toDate(valueToUse)
        } else if (valueToUse is Collection<*>) {
            valueToUse = valueToUse
                .filter { v -> if (v is Optional<*>) v.isPresent else true }
                .map { v -> mapValue(v) }
                .toList()
        } else if (valueToUse is Map<*, *>) {
            valueToUse = doc(valueToUse as Map<String, Any?>)
        } else if (valueToUse is URI) {
            valueToUse = valueToUse.toString()
        }

        return valueToUse
    }
}