package mtymes.tasks.samples.sampleXX

import com.mongodb.client.MongoCollection
import com.mongodb.client.model.IndexOptions
import javafixes.concurrency.Runner
import mtymes.tasks.common.domain.WorkerId
import mtymes.tasks.common.mongo.DocBuilder
import mtymes.tasks.common.mongo.DocBuilder.Companion.doc
import mtymes.tasks.common.time.Durations
import mtymes.tasks.scheduler.dao.GenericScheduler
import mtymes.tasks.scheduler.dao.SchedulerDefaults
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.CAN_BE_EXECUTED_AS_OF
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.DELETABLE_AFTER
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.EXECUTION_ATTEMPTS_LEFT
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.EXECUTION_ID
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.IS_PICKABLE
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.LAST_EXECUTION
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.PREVIOUS_EXECUTIONS
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.STATUS
import mtymes.tasks.scheduler.domain.*
import mtymes.tasks.test.mongo.emptyLocalCollection
import org.bson.Document
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong


class SimpleTaskDao(
    tasksCollection: MongoCollection<Document>
) {
    private val scheduler = GenericScheduler(
        collection = tasksCollection,
        defaults = SchedulerDefaults(

            submitTaskOptions = SubmitTaskOptions(
                ttl = Durations.SEVEN_DAYS
            ),

            pickNextExecutionOptions = PickNextExecutionOptions(
                keepAliveFor = Durations.FIVE_MINUTES
            )
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


    fun pickNextTaskExecution(
        workerId: WorkerId
    ): PickedExecutionSummary? {
        return scheduler.pickNextAvailableExecution(workerId)
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


object InsertionAndPickingPerformance {

    @JvmStatic
    fun main(args: Array<String>) {
        val itemCount = 1_000_000

        println("Insertion And Picking:")

        for (insertionThreadCount in listOf(
//            1, 2, 3, 5,
            10
        )) {
            for (pickingThreadCount in listOf(
//                1, 2, 3, 5,
                10
            )) {
                println("\n")
                println("itemCount = ${itemCount}")
                println("writerThreadCount = ${insertionThreadCount}")
                println("pickThreadCount = ${insertionThreadCount}")
                println("\n")

                val coll = emptyLocalCollection("perfCheck")
                val dao = SimpleTaskDao(coll)


                createDefaultIndexes(coll)


                var insertCount = AtomicLong(0L)
                var insertTotalDuration = AtomicLong(0L)
                var insertMaxDuration = AtomicLong(-1L)

                val insertionRunner = Runner(insertionThreadCount)
                val pickingRunner = Runner(pickingThreadCount)
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

                    val pickingCounter = AtomicInteger(0)
                    val workerId = WorkerId("SomeWorker")

                    var pickCount = AtomicLong(0L)
                    var pickTotalDuration = AtomicLong(0L)
                    var pickMaxDuration = AtomicLong(-1L)

                    for (threadNo in 1..insertionThreadCount) {
                        pickingRunner.run { shutDownInfo ->
                            do {
                                val startTime = System.currentTimeMillis()
                                val result = dao.pickNextTaskExecution(workerId)
                                val duration = System.currentTimeMillis() - startTime

                                pickCount.addAndGet(1)
                                pickTotalDuration.addAndGet(duration)
                                if (pickMaxDuration.get() < duration) {
                                    pickMaxDuration.set(duration)
                                }

                                if (result != null) {
                                    pickingCounter.incrementAndGet()
                                }
                            } while (pickingCounter.get() < itemCount)
                        }
                    }


                    insertionRunner.waitTillDone()
                    pickingRunner.waitTillDone()

                    println("- taskCount = ${coll.countDocuments()}")
                    println("- totalInsertDuration = ${insertTotalDuration.get()}")
                    println("- avgInsertDuration = ${insertTotalDuration.get().toDouble() / insertCount.get().toDouble()}")
                    println("- maxInsertDuration = ${insertMaxDuration.get()}")

                    println("- pickCount = ${pickCount.get()}")
                    println("- totalPickDuration = ${pickTotalDuration.get()}")
                    println("- avgPickDuration = ${pickTotalDuration.get().toDouble() / pickCount.get().toDouble()}")
                    println("- maxPickDuration = ${pickMaxDuration.get()}")

                } finally {
                    insertionRunner.shutdownNow()
                    pickingRunner.shutdownNow()
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
                IS_PICKABLE to true,
                STATUS to TaskStatus.available
            )
        )
    )
    coll.createIndex(
        doc(LAST_EXECUTION + "." + EXECUTION_ID to 1)
    )
    coll.createIndex(
        doc(DELETABLE_AFTER to 1),
        IndexOptions().expireAfter(
            0L, TimeUnit.SECONDS
        )
    )
}
