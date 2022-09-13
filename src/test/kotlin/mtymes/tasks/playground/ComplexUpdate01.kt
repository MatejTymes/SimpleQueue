package mtymes.tasks.playground

import com.mongodb.MongoClient
import mtymes.tasks.common.mongo.DocBuilder.Companion.doc
import mtymes.tasks.common.mongo.DocBuilder.Companion.emptyDoc
import mtymes.tasks.test.task.TaskViewer.displayTasksSummary
import java.util.UUID.randomUUID


object ComplexUpdate01 {

    @JvmStatic
    fun main(args: Array<String>) {
        val mongoClient = MongoClient("localhost", 27017)

        val db = mongoClient.getDatabase("sampleDB")

        val items = db.getCollection("items")

        items.deleteMany(emptyDoc())

        val id = randomUUID()

        val eId1 = randomUUID()
        val eId2 = randomUUID()

        items.insertOne(
            doc(
                "_id" to id,
                "attemptsLeft" to 1,
                "state" to "inProgress",

                "number" to 35,
//                "duration" to 12345,
                "executions" to listOf(
                    doc(
                        "id" to eId1,
                        "status" to "died"
                    ),
                    doc(
                        "id" to eId2,
                        "status" to "started"
                    )
                ),
                "lastExecutionId" to eId2
            )
        )

        items.updateOne(
            doc(
                "_id" to id
            ),
            listOf(
                doc(
                    "\$set" to doc(
                        "state" to doc(
                            "\$cond" to listOf(
                                doc("\$gt" to listOf("\$attemptsLeft", 0)),
                                "available",
                                "failed"
                            )
                        ),
                        "executions" to doc(
                            "\$map" to doc(
                                "input" to "\$executions",
                                "as" to "m",
                                "in" to doc(
                                    "\$cond" to listOf(
                                        doc("\$eq" to listOf("\$\$m.id", eId2)),
                                        doc("\$mergeObjects" to listOf("\$\$m", doc(
                                            "status" to "failed",
                                            "finishedAt" to "now"
                                        ))),
                                        "\$\$m"
                                    )
                                )
                            )
                        ),
                        "num2" to "\$number",
                        "num3" to doc("\$sum" to listOf("\$number", 5)),
                        "num4" to doc("\$add" to listOf("\$number", 2)),
                    )
                )
            )
        )

        displayTasksSummary(
            collection = items
        ) { true }

//        items.find().forEach {
//            println("- ${it}")
//        }
//        println(items.count())
    }
}