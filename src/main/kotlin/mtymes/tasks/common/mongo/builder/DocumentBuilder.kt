package mtymes.tasks.common.mongo.builder

import org.bson.Document

// todo: mtymes - rename to DocBuilder
class DocumentBuilder(
    private val registry: MongoWriterRegistry
) {

    private val values = linkedMapOf<String, Any?>()

    fun build(): Document {
        return Document(values)
    }

    fun put(key: String, value: Any?): DocumentBuilder {
        registry
            .findWriterFor(value)
            .writeValue(value, MapValueInserter(values, key), registry)
        return this
    }

    fun put(keyValue: Pair<String, Any?>): DocumentBuilder {
        return put(keyValue.first, keyValue.second)
    }

    fun putIf(condition: Boolean, key: String, value: Any?): DocumentBuilder {
        if (condition) {
            put(key, value)
        }
        return this
    }

    fun putIf(condition: Boolean, keyValue: Pair<String, Any?>): DocumentBuilder {
        return putIf(condition, keyValue.first, keyValue.second)
    }

    fun putIf(condition: Boolean, keyValueGenerator: () -> Pair<String, Any?>): DocumentBuilder {
        if (condition) {
            val (key, value) = keyValueGenerator.invoke()
            put(key, value)
        }
        return this
    }

    fun putAll(vararg pairs: Pair<String, Any?>): DocumentBuilder {
        val valueInserter = MapValueInserter(values)
        for ((key, value) in pairs) {
            valueInserter.changeFieldName(key)
            registry
                .findWriterFor(value)
                .writeValue(value, valueInserter, registry)
        }
        return this
    }

    fun putAll(valuesMap: Map<String, Any?>?): DocumentBuilder {
        if (valuesMap != null) {
            val valueInserter = MapValueInserter(values)
            for ((key, value) in valuesMap) {
                valueInserter.changeFieldName(key)
                registry
                    .findWriterFor(value)
                    .writeValue(value, valueInserter, registry)
            }
        }
        return this
    }

    fun putAllIf(condition: Boolean, valuesGenerator: () -> Map<String, Any?>): DocumentBuilder {
        if (condition) {
            putAll(valuesGenerator.invoke())
        }
        return this
    }
}