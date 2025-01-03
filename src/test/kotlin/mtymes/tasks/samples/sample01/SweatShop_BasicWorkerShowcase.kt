package mtymes.tasks.samples.sample01

import mtymes.tasks.common.domain.WorkerId
import mtymes.tasks.common.domain.WorkerId.Companion.uniqueWorkerId
import mtymes.tasks.worker.Worker
import mtymes.tasks.worker.sweatshop.HumbleSweatShop
import mtymes.tasks.worker.sweatshop.ShutDownMode.*
import java.util.UUID.randomUUID
import java.util.concurrent.CopyOnWriteArrayList


class LazyWorker : Worker<String> {

    private val tasksToProcess = CopyOnWriteArrayList(listOf("A", "B", "C"))

    override fun pickAvailableWork(
        workerId: WorkerId
    ): String? {
        return tasksToProcess.removeFirstOrNull()
    }

    override fun processWork(work: String, workerId: WorkerId) {
        // i'm lazy and just pretending i do something
        Thread.sleep(1_000)
    }
}



object WorkerDoingWork {

    @JvmStatic
    fun main(args: Array<String>) {

        HumbleSweatShop().use { sweatShop ->

            val worker = LazyWorker()

            sweatShop.addAndStartWorker(
                worker
            )

            sweatShop.close(
                shutDownMode = OnceNoMoreWork,
                waitTillDone = true
            )
        }
    }
}



object CustomWorkerId {

    @JvmStatic
    fun main(args: Array<String>) {

        HumbleSweatShop().use { sweatShop ->

            val worker = LazyWorker()

            sweatShop.addAndStartWorker(
                worker = worker,
                workerId = uniqueWorkerId("LazyDrone")
            )

            sweatShop.addAndStartWorker(
                worker = worker,
                workerId = WorkerId("DobbyTheElf")
            )

            sweatShop.close(
                shutDownMode = OnceNoMoreWork,
                waitTillDone = true
            )
        }
    }
}



object MultipleWorkersRegistered {

    @JvmStatic
    fun main(args: Array<String>) {

        HumbleSweatShop().use { sweatShop ->

            val worker1 = LazyWorker()
            val worker2 = LazyWorker()

            sweatShop.addAndStartWorker(worker1)
            sweatShop.addAndStartWorker(worker2)

            Thread.sleep(250)
            sweatShop.workerSummaries().forEach { summary ->
                println("- ${summary}")
            }

            sweatShop.close(
                shutDownMode = OnceNoMoreWork,
                waitTillDone = true
            )
        }
    }
}



object OneWorkerRegisteredMultipleTimes {

    @JvmStatic
    fun main(args: Array<String>) {

        HumbleSweatShop().use { sweatShop ->

            val worker = LazyWorker()

            sweatShop.addAndStartWorker(worker)
            sweatShop.addAndStartWorker(worker)

            Thread.sleep(250)
            sweatShop.workerSummaries().forEach { summary ->
                println("- ${summary}")
            }

            Thread.sleep(1_000)
            sweatShop.workerSummaries().forEach { summary ->
                println("- ${summary}")
            }

            sweatShop.close(
                shutDownMode = OnceNoMoreWork,
                waitTillDone = true
            )
        }
    }
}



object ClosingSweatShopTooEarly {

    @JvmStatic
    fun main(args: Array<String>) {

        HumbleSweatShop().use { sweatShop ->

            val worker = LazyWorker()

            sweatShop.addAndStartWorker(worker)

            Thread.sleep(1_250)
        }
    }
}



object InterruptWorker {

    @JvmStatic
    fun main(args: Array<String>) {

        HumbleSweatShop().use { sweatShop ->

            val worker = LazyWorker()

            val workerId = sweatShop.addAndStartWorker(worker)

            Thread.sleep(1_250)

            sweatShop.stopWorker(
                workerId = workerId,
                shutDownMode = Immediately
            )

            sweatShop.workerSummaries().forEach { summary ->
                println("- ${summary}")
            }

            Thread.sleep(1_250)
        }
    }
}



object InterruptWorkerGracefullyOnTaskFinish {

    @JvmStatic
    fun main(args: Array<String>) {

        HumbleSweatShop().use { sweatShop ->

            val workerId = sweatShop.addAndStartWorker(
                LazyWorker()
            )

            Thread.sleep(1_250)

            sweatShop.stopWorker(
                workerId = workerId,
                shutDownMode = OnceCurrentWorkIsFinished
            )

            sweatShop.workerSummaries().forEach { summary ->
                println("- ${summary}")
            }


            sweatShop.close(
                shutDownMode = OnceNoMoreWork,
                waitTillDone = true
            )


            sweatShop.workerSummaries().forEach { summary ->
                println("- ${summary}")
            }
        }
    }
}



object InterruptWorkerGracefullyOnceNoMoreWork {

    @JvmStatic
    fun main(args: Array<String>) {

        HumbleSweatShop().use { sweatShop ->

            val workerId = sweatShop.addAndStartWorker(
                LazyWorker()
            )

            Thread.sleep(1_250)

            sweatShop.stopWorker(
                workerId = workerId,
                shutDownMode = OnceNoMoreWork
            )

            sweatShop.workerSummaries().forEach { summary ->
                println("- ${summary}")
            }


            sweatShop.close(
                shutDownMode = OnceNoMoreWork,
                waitTillDone = true
            )

            sweatShop.workerSummaries().forEach { summary ->
                println("- ${summary}")
            }
        }
    }
}



object CloseableWorker : Worker<String> {
    override fun pickAvailableWork(workerId: WorkerId): String? {
        return randomUUID().toString()
    }

    override fun processWork(work: String, workerId: WorkerId) {
        println("Starting task")
        Thread.sleep(10_000)
    }

    override fun close() {
        println("Me closing! You're fine with that ???")
    }
}

object CloseTheWorker {

    @JvmStatic
    fun main(args: Array<String>) {

        println("Immediate shutdown")

        HumbleSweatShop().use { sweatShop ->
            sweatShop.addAndStartWorker(
                CloseableWorker
            )

            Thread.sleep(2500)
        }

        println("Graceful shutdown")

        HumbleSweatShop().use { sweatShop ->
            sweatShop.addAndStartWorker(
                CloseableWorker
            )

            Thread.sleep(2500)
            sweatShop.close(
                shutDownMode = OnceCurrentWorkIsFinished,
                waitTillDone = true
            )
        }

        println("Graceful & then Immediate shutdown")

        HumbleSweatShop().use { sweatShop ->
            sweatShop.addAndStartWorker(
                CloseableWorker
            )

            Thread.sleep(2500)
            sweatShop.close(
                shutDownMode = OnceCurrentWorkIsFinished,
                waitTillDone = false
            )
        }

        println("Graceful -> Graceful -> Immediate")

        HumbleSweatShop().use { sweatShop ->
            sweatShop.addAndStartWorker(
                CloseableWorker
            )

            Thread.sleep(2500)
            sweatShop.close(
                shutDownMode = OnceNoMoreWork,
                waitTillDone = false
            )

            Thread.sleep(2500)
            sweatShop.close(
                shutDownMode = OnceCurrentWorkIsFinished,
                waitTillDone = false
            )

            Thread.sleep(2500)
            sweatShop.close(
                shutDownMode = Immediately,
                waitTillDone = false
            )

            Thread.sleep(2500)
            sweatShop.close(
                shutDownMode = Immediately,
                waitTillDone = false
            )
        }
    }
}