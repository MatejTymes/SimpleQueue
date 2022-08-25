package mtymes.legacy.common.network

import java.net.InetAddress
import java.net.UnknownHostException

object HostUtil {
    fun longLocalHostName(): String {
        try {
            return InetAddress.getLocalHost().canonicalHostName.toLowerCase()
        } catch (e: UnknownHostException) {
            return "unknownHost"
        }
    }
}