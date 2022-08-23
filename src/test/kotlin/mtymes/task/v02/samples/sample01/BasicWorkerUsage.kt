package mtymes.task.v02.samples.sample01

import mtymes.task.v02.scheduler.domain.WorkerId
import mtymes.task.v02.worker.Worker
import mtymes.task.v02.worker.sweatshop.HumbleSweatShop
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
                workerId = WorkerId("LazyDrone")
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

            Thread.sleep(4_000)
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

            Thread.sleep(1_250)
        }
    }
}