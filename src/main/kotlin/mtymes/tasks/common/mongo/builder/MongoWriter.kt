package mtymes.tasks.common.mongo.builder

import javafixes.`object`.Microtype
import mtymes.tasks.common.time.DateUtil.toDate
import org.bson.Document
import java.time.ZonedDateTime
import java.util.*


interface MongoWriter<T> {

    fun writeValue(value: T?, valueInserter: ValueInserter, writerRegistry: MongoWriterRegistry)
}


object BaseMongoWriters {

    fun defaultMongoWriter(): MongoWriter<Any> {
        return PassTroughWriter
    }

    fun mongoWriters(): Map<Class<*>, MongoWriter<*>> {
        return mapOf(
            Document::class.java to PassTroughWriter,
            Map::class.java to MapWriter,
            Iterable::class.java to IterableWriter,
            Enum::class.java to EnumWriter,
            Optional::class.java to OptionalWriter,
            UUID::class.java to UUIDWriter,
            ZonedDateTime::class.java to ZonedDateTimeWriter,
            Microtype::class.java to MicrotypeWriter,

            // not actually needed to be defined (as the default is the PassThroughWriter, but decreases the class hierarchy traversing time
            java.lang.Boolean::class.java to PassTroughWriter,
            java.lang.String::class.java to PassTroughWriter,
            java.lang.Integer::class.java to PassTroughWriter,
            java.lang.Long::class.java to PassTroughWriter,
            java.lang.Short::class.java to PassTroughWriter,
            java.lang.Float::class.java to PassTroughWriter,
            java.lang.Double::class.java to PassTroughWriter,
            java.lang.Byte::class.java to PassTroughWriter,
            java.lang.Character::class.java to PassTroughWriter,
            java.util.Date::class.java to PassTroughWriter,

            // not actually needed to be defined (as the default Iterable is already handled by IterableWriter, but decreases the class hierarchy traversing time
            List::class.java to IterableWriter,
            Set::class.java to IterableWriter,
            Queue::class.java to IterableWriter,
            Collection::class.java to IterableWriter,
        )
    }
}


object UnsupportedWriter : MongoWriter<Any> {

    override fun writeValue(value: Any?, valueInserter: ValueInserter, writerRegistry: MongoWriterRegistry) {
        throw UnsupportedOperationException("Don't know how to write the value '${value}'")
    }
}


object PassTroughWriter : MongoWriter<Any> {

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


object ZonedDateTimeWriter : MongoWriter<ZonedDateTime> {

    override fun writeValue(value: ZonedDateTime?, valueInserter: ValueInserter, writerRegistry: MongoWriterRegistry) {
        if (value == null) {
            valueInserter.insertValue(null)
        } else {
            valueInserter.insertValue(toDate(value))
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
                mapInserter.changeFieldName(mapKey.toString())
                writerRegistry
                    .findWriterFor(mapValue)
                    .writeValue(mapValue, mapInserter, writerRegistry)
            }

            valueInserter.insertValue(document)
        }
    }
}


object IterableWriter : MongoWriter<Iterable<*>> {

    override fun writeValue(value: Iterable<*>?, valueInserter: ValueInserter, writerRegistry: MongoWriterRegistry) {
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
