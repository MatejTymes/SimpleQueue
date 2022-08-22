package mtymes.task.v02.samples

import mtymes.task.v02.scheduler.domain.WorkerId
import mtymes.task.v02.worker.TaskWorker
import mtymes.task.v02.worker.sweatshop.HumbleSweatShop



class Sample01_LazyWorker : TaskWorker<String> {

    private val tasksToProcess = mutableListOf("A", "B", "C")

    override fun fetchNextTaskToProcess(workerId: WorkerId): String? {
        return if (tasksToProcess.isEmpty()) {
            null
        } else {
            tasksToProcess.removeAt(0)
        }
    }

    override fun executeTask(task: String, workerId: WorkerId) {
        // i'm lazy and just pretending i do something
        Thread.sleep(1_000)
    }
}



object Sample01_WorkerDoingWork {

    @JvmStatic
    fun main(args: Array<String>) {

        HumbleSweatShop().use { sweatShop ->

            sweatShop.addAndStartWorker(
                Sample01_LazyWorker()
            )

            Thread.sleep(4_000)
        }
    }
}



object Sample01_ClosingSweatShopTooEarly {

    @JvmStatic
    fun main(args: Array<String>) {

        HumbleSweatShop().use { sweatShop ->

            sweatShop.addAndStartWorker(
                Sample01_LazyWorker()
            )

            Thread.sleep(1_250)
        }
    }
}



object Sample01_InterruptWorker {

    @JvmStatic
    fun main(args: Array<String>) {

        HumbleSweatShop().use { sweatShop ->

            val workerId = sweatShop.addAndStartWorker(
                Sample01_LazyWorker()
            )

            Thread.sleep(1_250)

            sweatShop.stopAndRemoveWorker(
                workerId = workerId,
                stopGracefully = false
            )

            Thread.sleep(1_250)
        }
    }
}



object Sample01_InterruptWorkerGracefully {

    @JvmStatic
    fun main(args: Array<String>) {

        HumbleSweatShop().use { sweatShop ->

            val workerId = sweatShop.addAndStartWorker(
                Sample01_LazyWorker()
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