package mtymes.tasks.beta.common.mongo.mappers

import mtymes.tasks.beta.common.mongo.DocumentBuilder
import org.bson.Document

interface WithDocumentBuilder {

    fun mongoWriterRegistry(): MongoWriterRegistry = CoreMongoWriterRegistry

    fun docBuilder(): DocumentBuilder = DocumentBuilder(mongoWriterRegistry())

    fun docBuilder(vararg pairs: Pair<String, Any?>): DocumentBuilder = docBuilder().putAll(*pairs)

    fun docBuilder(valuesMap: Map<String, Any?>): DocumentBuilder = docBuilder().putAll(valuesMap)

    fun emptyDoc(): Document = docBuilder().build()

    fun doc(key: String, value: Any?): Document = docBuilder().put(key, value).build()

    fun doc(vararg pairs: Pair<String, Any?>): Document = docBuilder().putAll(*pairs).build()

    fun doc(valuesMap: Map<String, Any?>): Document = docBuilder().putAll(valuesMap).build()
}