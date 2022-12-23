package mtymes.tasks.beta.common.mongo.mappers

import javafixes.collection.LinkedArrayQueue
import javafixes.`object`.Microtype
import mtymes.tasks.beta.common.mongo.DocumentBuilder
import mtymes.tasks.beta.common.mongo.mappers.CoreMongoWriters.defaultMongoWriter
import mtymes.tasks.beta.common.mongo.mappers.CoreMongoWriters.mongoWriters
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZonedDateTime
import java.util.*
import java.util.UUID.randomUUID
import java.util.concurrent.TimeUnit


interface MongoWriterRegistry {
    fun <T> findWriterFor(value: T?): MongoWriter<in T> // throw NonRecoverableException("No writer found for value of type '${value::class.simpleName}'")
}


class ClassBasedMongoWriterRegistry(
    private val writers: Map<Class<*>, MongoWriter<*>>,
    private val defaultWriter: MongoWriter<in Any>?
) : MongoWriterRegistry {

    fun <T> registerWriter(supportedClass: Class<T>, writer: MongoWriter<in T>): ClassBasedMongoWriterRegistry {
        return ClassBasedMongoWriterRegistry(
            writers + (supportedClass to writer as MongoWriter<*>),
            defaultWriter
        )
    }

    fun <T> deregisterWriter(supportedClass: Class<T>): ClassBasedMongoWriterRegistry {
        return ClassBasedMongoWriterRegistry(
            writers - supportedClass,
            defaultWriter
        )
    }

    fun registerDefaultWriter(writer: MongoWriter<in Any>): ClassBasedMongoWriterRegistry {
        return ClassBasedMongoWriterRegistry(
            writers,
            writer
        )
    }

    fun deregisterDefaultWriter(): ClassBasedMongoWriterRegistry {
        return ClassBasedMongoWriterRegistry(
            writers,
            null
        )
    }

    override fun <T> findWriterFor(value: T?): MongoWriter<in T> {
        var writer: MongoWriter<*>? = null

        if (value != null) {

            var classToProcess: Class<*>? = value!!::class.java

            var interfacesToProcess: LinkedArrayQueue<Class<*>>? = null
            mainLoop@ while (classToProcess != null) {

                writer = writers.get(classToProcess)
                if (writer != null) {
                    break@mainLoop
                }

                if (interfacesToProcess == null) {
                    interfacesToProcess = LinkedArrayQueue()
                }
                interfacesToProcess.addAll(classToProcess.interfaces)
                while (!interfacesToProcess.isEmpty()) {
                    val interfaceToProcess = interfacesToProcess.poll()

                    writer = writers.get(interfaceToProcess)
                    if (writer != null) {
                        break@mainLoop
                    }

                    interfacesToProcess.addAll(interfaceToProcess.interfaces)
                }

                classToProcess = classToProcess.superclass
            }
        }

        if (writer == null) {
            writer = defaultWriter
        }

        return (writer ?: UnsupportedWriter) as MongoWriter<in T>
    }
}

object CoreMongoWriters {

    fun defaultMongoWriter(): MongoWriter<Any>? {
        return PassTroughWriter
    }

    fun mongoWriters(): Map<Class<*>, MongoWriter<*>> {
        return mapOf(
            Map::class.java to MapWriter,
            Collection::class.java to CollectionWriter,
            Enum::class.java to EnumWriter,
            Optional::class.java to OptionalWriter,
            UUID::class.java to UUIDWriter,
            ZonedDateTime::class.java to ZonedDateTimeWriter,
            LocalDateTime::class.java to LocalDateTimeWriter,
            LocalDate::class.java to LocalDateWriter,
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
            Date::class.java to PassTroughWriter,

            // not actually needed to be defined (as the default Collection is already handled by CollectionWriter, but decreases the class hierarchy traversing time
            List::class.java to CollectionWriter,
            Set::class.java to CollectionWriter,
        )
    }
}

object CoreMongoWriterRegistry : MongoWriterRegistry {

    private val wrappedRegistry = ClassBasedMongoWriterRegistry(
        mongoWriters(),
        defaultMongoWriter()
    )

    override fun <T> findWriterFor(value: T?): MongoWriter<in T> {
        return wrappedRegistry.findWriterFor(value)
    }
}


fun main() {
    val registry: MongoWriterRegistry = CoreMongoWriterRegistry

    val docBuilder = DocumentBuilder(registry)

    docBuilder.putAll(
        "int" to 5,
        "list" to listOf(Optional.empty<String>(), 1, "Hello", TimeUnit.MILLISECONDS),
        "uuid" to randomUUID()
    )

    val doc = docBuilder.build()

    println("doc = ${doc}")
}