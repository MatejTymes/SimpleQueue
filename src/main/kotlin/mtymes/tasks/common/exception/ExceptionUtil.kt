package mtymes.tasks.common.exception

object ExceptionUtil {

    fun runAndIgnoreExceptions(code: () -> Unit) {
        try {
            code.invoke()
        } catch (e: Exception) {
            // ignore
        }
    }
}