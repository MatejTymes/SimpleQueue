package mtymes.tasks.common.mongo.builder

import org.bson.Document

class DocBuilder(
    private val registry: MongoWriterRegistry
) {

    private val values = linkedMapOf<String, Any?>()

    fun build(): Document {
        return Document(values)
    }

    fun put(key: String, value: Any?): DocBuilder {
        registry
            .findWriterFor(value)
            .writeValue(value, MapValueInserter(values, key), registry)
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
        val valueInserter = MapValueInserter(values)
        for ((key, value) in pairs) {
            valueInserter.changeFieldName(key)
            registry
                .findWriterFor(value)
                .writeValue(value, valueInserter, registry)
        }
        return this
    }

    fun putAll(valuesMap: Map<String, Any?>?): DocBuilder {
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

    fun putAllIf(condition: Boolean, valuesGenerator: () -> Map<String, Any?>): DocBuilder {
        if (condition) {
            putAll(valuesGenerator.invoke())
        }
        return this
    }
}