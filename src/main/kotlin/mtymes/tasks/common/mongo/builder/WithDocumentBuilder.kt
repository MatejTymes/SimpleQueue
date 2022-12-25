package mtymes.tasks.common.mongo.builder

import org.bson.Document


interface WithDocumentBuilder {

    fun mongoWriterRegistry(): MongoWriterRegistry

    fun docBuilder(): DocumentBuilder = DocumentBuilder(mongoWriterRegistry())

    fun docBuilder(vararg pairs: Pair<String, Any?>): DocumentBuilder = docBuilder().putAll(*pairs)

    fun docBuilder(valuesMap: Map<String, Any?>): DocumentBuilder = docBuilder().putAll(valuesMap)

    fun emptyDoc(): Document = docBuilder().build()

    fun doc(key: String, value: Any?): Document = docBuilder().put(key, value).build()

    fun doc(vararg pairs: Pair<String, Any?>): Document = docBuilder().putAll(*pairs).build()

    fun doc(valuesMap: Map<String, Any?>): Document = docBuilder().putAll(valuesMap).build()
}


interface WithCoreDocumentBuilder : WithDocumentBuilder {
    override fun mongoWriterRegistry(): MongoWriterRegistry = CoreMongoWriterRegistry
}