package mtymes.tasks.samples.sampleXX

import com.mongodb.client.MongoCollection
import com.mongodb.client.model.IndexOptions
import javafixes.collection.LinkedArrayQueue
import javafixes.concurrency.Runner
import mtymes.tasks.common.collection.QueueExt.pollingIterator
import mtymes.tasks.common.domain.WorkerId
import mtymes.tasks.common.mongo.builder.WithBaseDocBuilder
import mtymes.tasks.common.time.Durations
import mtymes.tasks.samples.sampleXX.IndexCreation.createDefaultIndexes
import mtymes.tasks.scheduler.dao.GenericScheduler
import mtymes.tasks.scheduler.dao.SchedulerDefaults
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.CAN_BE_EXECUTED_AS_OF
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.DELETABLE_AFTER
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.EXECUTION_ID
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.IS_PICKABLE
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.LAST_EXECUTION
import mtymes.tasks.scheduler.dao.UniversalScheduler.Companion.STATUS
import mtymes.tasks.scheduler.domain.*
import mtymes.tasks.test.mongo.emptyLocalCollection
import org.bson.Document
import java.util.*
import java.util.Collections.sort
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong


class SimpleTaskDao(
    tasksCollection: MongoCollection<Document>
) : WithBaseDocBuilder {
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
            doc(
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
        val itemCount = 500_000

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
        val itemCount = 500_000

        println("Insertion And Picking:")

        for (insertionThreadCount in listOf(
//            1, 2, 3, 5,
            10
        )) {
            for (pickingThreadCount in listOf(
//                1,
//                2,
//                3,
//                5,
//                10,
//                15,
                20
            )) {
                println("\n")
                println("itemCount = ${itemCount}")
                println("writerThreadCount = ${insertionThreadCount}")
                println("pickThreadCount = ${insertionThreadCount}")
                println("\n")

                val coll = emptyLocalCollection("perfCheck")
                val dao = SimpleTaskDao(coll)


                createDefaultIndexes(coll)


                val insertDurations = mutableListOf<Queue<Long>>()
                val pickDurations = mutableListOf<Queue<Long>>()

                val insertionRunner = Runner(insertionThreadCount)
                val pickingRunner = Runner(pickingThreadCount)
                try {
                    val insertionCounter = AtomicInteger(1)

                    for (threadNo in 1..insertionThreadCount) {
                        val durationsQueue = LinkedArrayQueue<Long>(256)
                        insertDurations.add(durationsQueue)

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

                                durationsQueue.add(duration)
                            }
                        }
                    }

                    val workerId = WorkerId("SomeWorker")

                    for (threadNo in 1..insertionThreadCount) {
                        val durationsQueue = LinkedArrayQueue<Long>(256)
                        pickDurations.add(durationsQueue)

                        pickingRunner.run { shutDownInfo ->
                            do {
                                val startTime = System.currentTimeMillis()
                                val result = dao.pickNextTaskExecution(workerId)
                                val duration = System.currentTimeMillis() - startTime

                                if (result == null) {
                                    break
                                } else {
                                    durationsQueue.add(duration)
                                }
                            } while (true)
                        }
                    }


                    insertionRunner.waitTillDone()
                    pickingRunner.waitTillDone()


                    println("- taskCount = ${coll.countDocuments()}")
                    println()


                    val allInsertDurations = ArrayList<Long>(
                        insertDurations.sumOf { it.size }
                    )
                    insertDurations.forEach {
                        it.pollingIterator().forEach { allInsertDurations.add(it) }
                    }
                    println("INSERTION durations in ms")
                    showDurations(allInsertDurations)


                    val allPickDurations = ArrayList<Long>(
                        pickDurations.sumOf { it.size }
                    )
                    pickDurations.forEach {
                        it.pollingIterator().forEach { allPickDurations.add(it) }
                    }
                    sort(allPickDurations)
                    println("PICK durations in ms")
                    showDurations(allPickDurations)

                } finally {
                    insertionRunner.shutdownNow()
                    pickingRunner.shutdownNow()
                }
            }
        }
    }
}

private fun showDurations(allDurations: MutableList<Long>) {
    sort(allDurations)

    val totalDuration = allDurations.sum()
    val durationsCount = allDurations.size

    println("- total = ${totalDuration}")
    println("- avg = ${totalDuration.toDouble() / durationsCount.toDouble()}")
    println("- mean = ${allDurations.get(durationsCount / 2)}")
    println("- 90 = ${allDurations.get(durationsCount * 90 / 100)}")
    println("- 95 = ${allDurations.get(durationsCount * 95 / 100)}")
    println("- 99 = ${allDurations.get(durationsCount * 99 / 100)}")
    println("- 99.9 = ${allDurations.get(durationsCount * 999 / 1000)}")
    println("- max = ${allDurations.last()}")
    println()
}


object IndexCreation : WithBaseDocBuilder {

    fun createDefaultIndexes(coll: MongoCollection<Document>) {
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
}
