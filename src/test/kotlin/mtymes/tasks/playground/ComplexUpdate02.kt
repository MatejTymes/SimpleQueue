package mtymes.tasks.playground

import com.mongodb.MongoClient
import mtymes.tasks.common.mongo.DocBuilder.Companion.doc
import mtymes.tasks.common.mongo.DocBuilder.Companion.emptyDoc
import mtymes.tasks.test.task.TaskViewer.displayTasksSummary
import org.bson.Document
import java.util.UUID.randomUUID


object ComplexUpdate02 {

    @JvmStatic
    fun main(args: Array<String>) {
        val mongoClient = MongoClient("localhost", 27017)

        val db = mongoClient.getDatabase("sampleDB")

        val items = db.getCollection("items")

        items.deleteMany(emptyDoc())

        val id = randomUUID()

        val eId1 = randomUUID()
        val eId2 = randomUUID()
        val eId3 = randomUUID()

        items.insertOne(
            doc(
                "_id" to id,
//                "prevExecutions" to listOf<Document>(),
                "prevExecutions" to listOf(
                    doc(
                        "id" to eId1,
                        "status" to "died"
                    )
                ),
                "lastExecution" to doc(
                    "id" to eId2,
                    "status" to "failed"
                ),
            )
        )

        items.updateOne(
            doc(
                "_id" to id
            ),
            listOf(
//                doc("\$push" to doc(
//                    "prevExecutions" to "\$lastExecution"
//                )),
                doc(
                    "\$set" to doc(
//                        "prevExecutions" to doc(
//                            "\$concatArrays" to listOf(
//                                "\$prevExecutions",
//                                listOf("\$lastExecution")
//                            )
//                        ),
                        "eval1" to doc("\$eq" to listOf("\$lastExecution", null)),
                        "eval2" to doc("\$type" to "\$lastExecution"),
                        "eval3" to doc("\$eq" to listOf(doc("\$type" to "\$lastExecution"), "object")),
                        "prevExecutions" to doc(
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
                        ),

                        "lastExecution" to doc(
                            "id" to eId3,
                            "status" to "running",
//                            "data" to emptyDoc()
                        )
                    )
                ),
//                doc("\$set" to doc("lastExecution.data", emptyDoc()))
            )
        )

        displayTasksSummary(
            collection = items
        )
    }
}
