package mtymes.task.v02.common.host

import java.net.InetAddress
import java.net.UnknownHostException

class HostUtil {
    companion object {
        fun longLocalHostName(): String {
            try {
                return InetAddress.getLocalHost().canonicalHostName.lowercase()
            } catch (e: UnknownHostException) {
                return "unknownHost"
            }
        }

        fun shortLocalHostName(): String {
            val longLocalHostName = longLocalHostName()
            return if (longLocalHostName.indexOf(".") > 0) {
                longLocalHostName.substring(0, longLocalHostName.indexOf("."))
            } else {
                longLocalHostName
            }
        }
    }
}