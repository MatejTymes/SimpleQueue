package mtymes.task.v02.samples.sampleXX

import com.mongodb.client.MongoCollection
import com.mongodb.client.model.IndexOptions
import javafixes.concurrency.Runner
import mtymes.task.v02.common.mongo.DocBuilder
import mtymes.task.v02.common.mongo.DocBuilder.Companion.doc
import mtymes.task.v02.scheduler.dao.GenericTaskScheduler
import mtymes.task.v02.scheduler.dao.SchedulerDefaults
import mtymes.task.v02.scheduler.dao.UniversalScheduler.Companion.CAN_BE_EXECUTED_AS_OF
import mtymes.task.v02.scheduler.dao.UniversalScheduler.Companion.DELETE_AFTER
import mtymes.task.v02.scheduler.dao.UniversalScheduler.Companion.EXECUTIONS
import mtymes.task.v02.scheduler.dao.UniversalScheduler.Companion.EXECUTION_ATTEMPTS_LEFT
import mtymes.task.v02.scheduler.dao.UniversalScheduler.Companion.EXECUTION_ID
import mtymes.task.v02.scheduler.dao.UniversalScheduler.Companion.STATUS
import mtymes.task.v02.scheduler.domain.TaskId
import mtymes.task.v02.scheduler.domain.TaskStatus
import mtymes.task.v02.test.mongo.emptyLocalCollection
import org.bson.Document
import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong


class SimpleTaskDao(
    tasksCollection: MongoCollection<Document>
) {
    private val scheduler = GenericTaskScheduler(
        collection = tasksCollection,
        defaults = SchedulerDefaults(
            ttlDuration = Duration.ofDays(7),
            afterStartKeepAliveFor = Duration.ofMinutes(5)
        )
    )

    fun submitTask(
        request: String
    ): TaskId {
        return scheduler.submitTask(
            DocBuilder.doc(
                "request" to request
            )
        )
    }
}


object InsertionOnlyPerformance {

    @JvmStatic
    fun main(args: Array<String>) {
        val itemCount = 100_000

        println("Insertion Only:")

        for (threadCount in listOf(1, 2, 3, 5, 10)) {
            println("\n\nwriterThreadCount = ${threadCount}\n")

            val coll = emptyLocalCollection("perfCheck")
            val dao = SimpleTaskDao(coll)


            createDefaultIndexes(coll)


            var insertionCount = AtomicLong(0L)
            var totalDuration = AtomicLong(0L)
            var maxDuration = AtomicLong(-1L)

            val runner = Runner(threadCount)
            try {
                val i = AtomicInteger(1)

                runner.run { shutDownInfo ->
                    while(true) {
                        val number = i.getAndIncrement()
                        if (number > itemCount) {
                            break
                        }

                        val body = "myBodyNo${number}"

                        val startTime = System.currentTimeMillis()
                        dao.submitTask(body)
                        val duration = System.currentTimeMillis() - startTime

                        insertionCount.addAndGet(1)
                        totalDuration.addAndGet(duration)
                        if (maxDuration.get() < duration) {
                            maxDuration.set(duration)
                        }
                    }
                }

                runner.waitTillDone()

                println("- taskCount = ${coll.countDocuments()}")
                println("- totalDuration = ${totalDuration.get()}")
                println("- avgDuration = ${totalDuration.get().toDouble() / insertionCount.get().toDouble()}")
                println("- maxDuration = ${maxDuration.get()}")

            } finally {
                runner.shutdownNow()
            }
        }
    }
}


private fun createDefaultIndexes(coll: MongoCollection<Document>) {
    coll.createIndex(
        doc(
            CAN_BE_EXECUTED_AS_OF to 1
        ),
        IndexOptions().partialFilterExpression(
            doc(
                STATUS to TaskStatus.available,
                EXECUTION_ATTEMPTS_LEFT to doc("\$gte", 1)
            )
        )
    )
    coll.createIndex(
        doc(EXECUTIONS + "." + EXECUTION_ID to 1)
    )
    coll.createIndex(
        doc(DELETE_AFTER to 1),
        IndexOptions().expireAfter(
            0L, TimeUnit.SECONDS
        )
    )
}
