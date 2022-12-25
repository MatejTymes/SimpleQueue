package mtymes.tasks.playground

import com.mongodb.MongoClient
import mtymes.tasks.common.mongo.builder.WithCoreDocBuilder
import mtymes.tasks.test.task.TaskViewer.displayTasksSummary
import org.bson.Document
import java.util.UUID.randomUUID


object ComplexUpdate03 : WithCoreDocBuilder {

    @JvmStatic
    fun main(args: Array<String>) {
        val mongoClient = MongoClient("localhost", 27017)

        val db = mongoClient.getDatabase("sampleDB")

        val items = db.getCollection("items")

        items.deleteMany(emptyDoc())

        val id = randomUUID()

        val eId1 = "executionId01"
        val eId2 = "executionId02"

        items.insertOne(
            doc(
                "_id" to id,
                "retainOnlyLastExecution" to false,
                "prevExecutions" to listOf<Document>(),
                "lastExecution" to doc(
                    "id" to eId1,
                    "status" to "failed"
                ),

                )
        )

        items.updateOne(
            doc(
                "_id" to id
            ),
            listOf(
                doc(
                    "\$set" to doc(
                        "prevExecutions" to doc(
                            "\$cond" to listOf(
                                doc("\$eq" to listOf("\$retainOnlyLastExecution", true)),
                                "NOT RECORDED",
                                doc(
                                    "\$cond" to listOf(
                                        doc("\$eq" to listOf(doc("\$type" to "\$lastExecution"), "object")),
                                        doc(
                                            "\$concatArrays" to listOf(
                                                "\$prevExecutions",
                                                listOf("\$lastExecution")
                                            )
                                        ),
                                        listOf<Document>()
                                    )
                                )
                            )
                        ),

                        "lastExecution" to doc(
                            "id" to eId2,
                            "status" to "running",
//                            "data" to emptyDoc()
                        )
                    )
                ),
            )
        )

        displayTasksSummary(
            collection = items
        )
    }
}
