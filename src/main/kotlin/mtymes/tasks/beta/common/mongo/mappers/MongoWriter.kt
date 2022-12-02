package mtymes.tasks.beta.common.mongo.mappers

import javafixes.`object`.Microtype
import org.bson.Document
import java.util.*


interface MongoWriter<T> {

    fun writeValue(value: T?, valueInserter: ValueInserter, writerRegistry: MongoWriterRegistry)
}


object UnsupportedWriter : MongoWriter<Any> {

    override fun writeValue(value: Any?, valueInserter: ValueInserter, writerRegistry: MongoWriterRegistry) {
        throw UnsupportedOperationException("Don't know how to write the value '${value}'")
    }
}


object AnyWriter : MongoWriter<Any> {

    override fun writeValue(value: Any?, valueInserter: ValueInserter, writerRegistry: MongoWriterRegistry) {
        valueInserter.insertValue(value)
    }
}


object EnumWriter : MongoWriter<Enum<*>> {

    override fun writeValue(value: Enum<*>?, valueInserter: ValueInserter, writerRegistry: MongoWriterRegistry) {
        valueInserter.insertValue(value?.name)
    }
}


object UUIDWriter : MongoWriter<UUID> {

    override fun writeValue(value: UUID?, valueInserter: ValueInserter, writerRegistry: MongoWriterRegistry) {
        valueInserter.insertValue(value?.toString())
    }
}


object OptionalWriter : MongoWriter<Optional<*>> {

    override fun writeValue(value: Optional<*>?, valueInserter: ValueInserter, writerRegistry: MongoWriterRegistry) {
        if (value == null) {
            valueInserter.insertValue(null)
        } else if (!value.isPresent) {
            // do nothing
        } else {
            val underlyingValue = value.get()

            writerRegistry.findWriterFor(underlyingValue)
                .writeValue(underlyingValue, valueInserter, writerRegistry)
        }
    }
}


object MicrotypeWriter : MongoWriter<Microtype<*>> {

    override fun writeValue(value: Microtype<*>?, valueInserter: ValueInserter, writerRegistry: MongoWriterRegistry) {
        if (value == null) {
            valueInserter.insertValue(null)
        } else {
            val underlyingValue = value.value

            writerRegistry.findWriterFor(underlyingValue)
                .writeValue(underlyingValue, valueInserter, writerRegistry)
        }
    }
}


object MapWriter : MongoWriter<Map<*, *>> {

    override fun writeValue(value: Map<*, *>?, valueInserter: ValueInserter, writerRegistry: MongoWriterRegistry) {
        if (value == null) {
            valueInserter.insertValue(null)
        } else {
            val document = Document()

            val mapInserter = DocumentInserter(document)
            value.forEach { (mapKey, mapValue) ->
                mapInserter.setFieldName(mapKey.toString())
                writerRegistry
                    .findWriterFor(mapValue)
                    .writeValue(mapValue, mapInserter, writerRegistry)
            }

            valueInserter.insertValue(document)
        }
    }
}


object CollectionWriter : MongoWriter<Collection<*>> {

    override fun writeValue(value: Collection<*>?, valueInserter: ValueInserter, writerRegistry: MongoWriterRegistry) {
        if (value == null) {
            valueInserter.insertValue(null)
        } else {
            val list = mutableListOf<Any?>()

            val listInserter = ListValueInserter(list)
            value.forEach {
                val item = it
                writerRegistry
                    .findWriterFor(item)
                    .writeValue(item, listInserter, writerRegistry)
            }

            valueInserter.insertValue(list)
        }
    }
}
