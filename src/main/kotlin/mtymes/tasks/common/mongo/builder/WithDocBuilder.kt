package mtymes.tasks.common.mongo.builder

import org.bson.Document


interface WithDocBuilder {

    fun mongoWriterRegistry(): MongoWriterRegistry

    fun docBuilder(): DocBuilder = DocBuilder(mongoWriterRegistry())

    fun docBuilder(vararg pairs: Pair<String, Any?>): DocBuilder = docBuilder().putAll(*pairs)

    fun docBuilder(valuesMap: Map<String, Any?>): DocBuilder = docBuilder().putAll(valuesMap)

    fun emptyDoc(): Document = docBuilder().build()

    fun doc(key: String, value: Any?): Document = docBuilder().put(key, value).build()

    fun doc(vararg pairs: Pair<String, Any?>): Document = docBuilder().putAll(*pairs).build()

    fun doc(valuesMap: Map<String, Any?>): Document = docBuilder().putAll(valuesMap).build()
}


interface WithCoreDocBuilder : WithDocBuilder {
    override fun mongoWriterRegistry(): MongoWriterRegistry = CoreMongoWriterRegistry
}

object DefaultDocBuilder : WithCoreDocBuilder