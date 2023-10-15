package mtymes.tasks.common.mongo.builder

import javafixes.collection.LinkedArrayQueue


interface MongoWriterRegistry {
    fun <T> findWriterFor(value: T?): MongoWriter<in T> // throw NonRecoverableException("No writer found for value of type '${value::class.simpleName}'")
}


object BaseMongoWriterRegistry : MongoWriterRegistry {

    private val wrappedRegistry = ClassBasedMongoWriterRegistry(
        BaseMongoWriters.mongoWriters(),
        BaseMongoWriters.defaultMongoWriter()
    )

    override fun <T> findWriterFor(value: T?): MongoWriter<in T> {
        return wrappedRegistry.findWriterFor(value)
    }
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


//fun main() {
//    val registry: MongoWriterRegistry = CoreMongoWriterRegistry
//
//    val docBuilder = DocBuilder(registry)
//
//    docBuilder.putAll(
//        "int" to 5,
//        "list" to listOf(Optional.empty<String>(), 1, "Hello", TimeUnit.MILLISECONDS),
//        "uuid" to randomUUID()
//    )
//
//    val doc = docBuilder.build()
//
//    println("doc = ${doc}")
//}