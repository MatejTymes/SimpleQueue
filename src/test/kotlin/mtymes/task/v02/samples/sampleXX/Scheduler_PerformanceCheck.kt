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
import mtymes.task.v02.scheduler.domain.StartedExecutionSummary
import mtymes.task.v02.scheduler.domain.TaskId
import mtymes.task.v02.scheduler.domain.TaskStatus
import mtymes.task.v02.scheduler.domain.WorkerId
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


    fun fetchNextTaskExecution(
        workerId: WorkerId
    ): StartedExecutionSummary? {
        return scheduler.fetchNextAvailableExecution(workerId)
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

                for (thread in 1..threadCount) {
                    runner.run { shutDownInfo ->
                        while (true) {
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


object InsertionAndFetchingPerformance {

    @JvmStatic
    fun main(args: Array<String>) {
        val itemCount = 1_000_000

        println("Insertion And Fetching:")

        for (insertionThreadCount in listOf(
//            1, 2, 3, 5,
            10
        )) {
            for (fetchingThreadCount in listOf(
//                1, 2, 3, 5,
                10
            )) {
                println("\n")
                println("itemCount = ${itemCount}")
                println("writerThreadCount = ${insertionThreadCount}")
                println("fetchThreadCount = ${insertionThreadCount}")
                println("\n")

                val coll = emptyLocalCollection("perfCheck")
                val dao = SimpleTaskDao(coll)


                createDefaultIndexes(coll)


                var insertCount = AtomicLong(0L)
                var insertTotalDuration = AtomicLong(0L)
                var insertMaxDuration = AtomicLong(-1L)

                val insertionRunner = Runner(insertionThreadCount)
                val fetchingRunner = Runner(fetchingThreadCount)
                try {
                    val insertionCounter = AtomicInteger(1)

                    for (threadNo in 1..insertionThreadCount) {
                        insertionRunner.run { shutDownInfo ->
                            while (true) {
                                val number = insertionCounter.getAndIncrement()
                                if (number > itemCount) {
                                    break
                                }

                                val body = "myBodyNo${number}"

                                val startTime = System.currentTimeMillis()
                                dao.submitTask(body)
                                val duration = System.currentTimeMillis() - startTime

                                insertCount.addAndGet(1)
                                insertTotalDuration.addAndGet(duration)
                                if (insertMaxDuration.get() < duration) {
                                    insertMaxDuration.set(duration)
                                }
                            }
                        }
                    }

                    val fetchingCounter = AtomicInteger(0)
                    val workerId = WorkerId("SomeWorker")

                    var fetchCount = AtomicLong(0L)
                    var fetchTotalDuration = AtomicLong(0L)
                    var fetchMaxDuration = AtomicLong(-1L)

                    for (threadNo in 1..insertionThreadCount) {
                        fetchingRunner.run { shutDownInfo ->
                            do {
                                val startTime = System.currentTimeMillis()
                                val result = dao.fetchNextTaskExecution(workerId)
                                val duration = System.currentTimeMillis() - startTime

                                fetchCount.addAndGet(1)
                                fetchTotalDuration.addAndGet(duration)
                                if (fetchMaxDuration.get() < duration) {
                                    fetchMaxDuration.set(duration)
                                }

                                if (result != null) {
                                    fetchingCounter.incrementAndGet()
                                }
                            } while (fetchingCounter.get() < itemCount)
                        }
                    }


                    insertionRunner.waitTillDone()
                    fetchingRunner.waitTillDone()

                    println("- taskCount = ${coll.countDocuments()}")
                    println("- totalInsertDuration = ${insertTotalDuration.get()}")
                    println("- avgInsertDuration = ${insertTotalDuration.get().toDouble() / insertCount.get().toDouble()}")
                    println("- maxInsertDuration = ${insertMaxDuration.get()}")

                    println("- fetchCount = ${fetchCount.get()}")
                    println("- totalFetchDuration = ${fetchTotalDuration.get()}")
                    println("- avgFetchDuration = ${fetchTotalDuration.get().toDouble() / fetchCount.get().toDouble()}")
                    println("- maxFetchDuration = ${fetchMaxDuration.get()}")

                } finally {
                    insertionRunner.shutdownNow()
                }
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
