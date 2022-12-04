package mtymes.tasks.beta.common.mongo.mappers

import javafixes.collection.LinkedArrayQueue
import javafixes.`object`.Microtype
import mtymes.tasks.beta.common.mongo.DocumentBuilder
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZonedDateTime
import java.util.*
import java.util.UUID.randomUUID
import java.util.concurrent.TimeUnit


interface MongoWriterRegistry {
    fun findWriterFor(value: Any?): MongoWriter<in Any> // throw NonRecoverableException("No writer found for value of type '${value::class.simpleName}'")
}


class ClassBasedMongoWriterRegistry(
    private val writers: Map<Class<*>, MongoWriter<in Any>>,
    private val defaultWriter: MongoWriter<in Any>?
) : MongoWriterRegistry {

    fun <T> registerWriter(supportedClass: Class<T>, writer: MongoWriter<in T>): ClassBasedMongoWriterRegistry {
        return ClassBasedMongoWriterRegistry(
            writers + (supportedClass to writer as MongoWriter<in Any>),
            defaultWriter
        )
    }

    fun registerDefaultWriter(writer: MongoWriter<in Any>): ClassBasedMongoWriterRegistry {
        return ClassBasedMongoWriterRegistry(
            writers,
            writer
        )
    }

    override fun findWriterFor(value: Any?): MongoWriter<in Any> {
        var writer: MongoWriter<in Any>? = null

        if (value != null) {

            var classToProcess: Class<*>? = value::class.java

            var interfacesToProcess: LinkedArrayQueue<Class<*>>? = null
            mainLoop@while (classToProcess != null) {

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

        return writer ?: UnsupportedWriter
    }
}

object DefaultMongoWriterRegisty : MongoWriterRegistry {

    private val wrappedRegistry = ClassBasedMongoWriterRegistry(emptyMap(), null)
        .registerDefaultWriter(PassTroughWriter)
        .registerWriter(Map::class.java, MapWriter)
        .registerWriter(Collection::class.java, CollectionWriter)
        .registerWriter(Enum::class.java, EnumWriter)
        .registerWriter(Optional::class.java, OptionalWriter)
        .registerWriter(UUID::class.java, UUIDWriter)
        .registerWriter(ZonedDateTime::class.java, ZonedDateTimeWriter)
        .registerWriter(LocalDateTime::class.java, LocalDateTimeWriter)
        .registerWriter(LocalDate::class.java, LocalDateWriter)
        .registerWriter(Microtype::class.java, MicrotypeWriter)
        // not actually needed to be defined (as the default is the PassThroughWriter) but decreases the class traversing time
        .registerWriter(java.lang.Boolean::class.java, PassTroughWriter)
        .registerWriter(java.lang.String::class.java, PassTroughWriter)
        .registerWriter(java.lang.Integer::class.java, PassTroughWriter)
        .registerWriter(java.lang.Long::class.java, PassTroughWriter)
        .registerWriter(java.lang.Short::class.java, PassTroughWriter)
        .registerWriter(java.lang.Float::class.java, PassTroughWriter)
        .registerWriter(java.lang.Double::class.java, PassTroughWriter)
        .registerWriter(java.lang.Byte::class.java, PassTroughWriter)
        .registerWriter(java.lang.Character::class.java, PassTroughWriter)

    override fun findWriterFor(value: Any?): MongoWriter<in Any> {
        return wrappedRegistry.findWriterFor(value)
    }
}


fun main() {
    val registry: MongoWriterRegistry = DefaultMongoWriterRegisty

    val docBuilder = DocumentBuilder(registry)

    docBuilder.putAll(
        "int" to 5,
        "list" to listOf(Optional.empty<String>(), 1, "Hello", TimeUnit.MILLISECONDS),
        "uuid" to randomUUID()
    )

    val doc = docBuilder.build()

    println("doc = ${doc}")
}