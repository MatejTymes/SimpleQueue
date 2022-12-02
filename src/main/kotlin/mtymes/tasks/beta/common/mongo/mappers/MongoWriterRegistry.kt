package mtymes.tasks.beta.common.mongo.mappers

import javafixes.collection.LinkedArrayQueue
import javafixes.`object`.Microtype
import org.bson.Document
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

    fun <T> registerWriter(supportedClass: Class<T>, writer: MongoWriter<T>): ClassBasedMongoWriterRegistry {
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
        .registerDefaultWriter(AnyWriter)
        .registerWriter(Enum::class.java, EnumWriter)
        .registerWriter(UUID::class.java, UUIDWriter)
        .registerWriter(Optional::class.java, OptionalWriter)
        .registerWriter(Microtype::class.java, MicrotypeWriter)
        .registerWriter(Map::class.java, MapWriter)
        .registerWriter(Collection::class.java, CollectionWriter)

    override fun findWriterFor(value: Any?): MongoWriter<in Any> {
        return wrappedRegistry.findWriterFor(value)
    }
}


fun main() {
    val registry: MongoWriterRegistry = DefaultMongoWriterRegisty

    val doc = Document()
    val docInserter = DocumentInserter(doc)

    val values = listOf(
        Optional.empty<String>(), 1, "Hello", TimeUnit.MILLISECONDS)

    docInserter.setFieldName("list")
    registry.findWriterFor(values).writeValue(values, docInserter, registry)

    docInserter.setFieldName("uuid")
    val uuid = randomUUID()
    registry.findWriterFor(uuid).writeValue(uuid, docInserter, registry)

    println(doc)
}