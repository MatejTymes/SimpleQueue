package mtymes.v02.common.host

import java.net.InetAddress
import java.net.UnknownHostException

class HostUtil {
    companion object {
        fun longLocalHostName(): String {
            try {
                return InetAddress.getLocalHost().canonicalHostName.toLowerCase()
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