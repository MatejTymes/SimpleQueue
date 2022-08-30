package mtymes.tasks.samples.sample01

import mtymes.tasks.common.domain.WorkerId
import mtymes.tasks.common.domain.WorkerId.Companion.uniqueWorkerId
import mtymes.tasks.worker.Worker
import mtymes.tasks.worker.sweatshop.HumbleSweatShop
import java.util.concurrent.CopyOnWriteArrayList


class LazyWorker : Worker<String> {

    private val tasksToProcess = CopyOnWriteArrayList(listOf("A", "B", "C"))

    override fun fetchNextTaskToProcess(
        workerId: WorkerId
    ): String? {
        return tasksToProcess.removeFirstOrNull()
    }

    override fun executeTask(task: String, workerId: WorkerId) {
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

            Thread.sleep(4_000)
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

            Thread.sleep(4_000)
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

            Thread.sleep(4_000)
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

            Thread.sleep(3_000)
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

            sweatShop.stopAndRemoveWorker(
                workerId = workerId,
                stopGracefully = false
            )

            sweatShop.workerSummaries().forEach { summary ->
                println("- ${summary}")
            }

            Thread.sleep(1_250)
        }
    }
}



object InterruptWorkerGracefully {

    @JvmStatic
    fun main(args: Array<String>) {

        HumbleSweatShop().use { sweatShop ->

            val workerId = sweatShop.addAndStartWorker(
                LazyWorker()
            )

            Thread.sleep(1_250)

            sweatShop.stopAndRemoveWorker(
                workerId = workerId,
                stopGracefully = true
            )

            sweatShop.workerSummaries().forEach { summary ->
                println("- ${summary}")
            }


            Thread.sleep(1_250)


            sweatShop.workerSummaries().forEach { summary ->
                println("- ${summary}")
            }
        }
    }
}