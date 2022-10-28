package mtymes.tasks.common.domain

import javafixes.`object`.Microtype
import mtymes.tasks.common.host.HostUtil
import org.apache.commons.lang3.RandomStringUtils
import java.util.concurrent.atomic.AtomicLong


class WorkerId(value: String) : Microtype<String>(value) {

    companion object {
        private val counter = AtomicLong(0L)
        private val uniqueAppPrefix = lazy { RandomStringUtils.randomAlphanumeric(3) }

        fun uniqueWorkerId(): WorkerId {
            return WorkerId("${HostUtil.shortLocalHostName()}-${uniqueSuffix()}")
        }

        fun uniqueWorkerId(taskName: String): WorkerId {
            return WorkerId("${HostUtil.shortLocalHostName()}-${taskName}-${counter.incrementAndGet()}")
        }

        private fun uniqueSuffix(): String {
            return "${uniqueAppPrefix.value}-${RandomStringUtils.randomAlphanumeric(3)}-${counter.incrementAndGet()}"
//            return counter.incrementAndGet().toString()
//            return UUID.randomUUID().toString()
//            return RandomStringUtils.randomAlphanumeric(8)
        }
    }
}
