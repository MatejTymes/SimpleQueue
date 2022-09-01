package mtymes.tasks.scheduler.exception

class ExecutionNotFoundException(message: String) : IllegalStateException(message)
class NotLastExecutionException(message: String) : IllegalStateException(message)

class UnexpectedStatusException(message: String) : IllegalStateException(message)

class TaskStatusAlreadyAppliedException(message: String) : java.lang.IllegalStateException(message)
class TaskAndExecutionStatusAlreadyAppliedException(message: String) : java.lang.IllegalStateException(message)

class UnknownFailureReasonException(message: String) : IllegalStateException(message)
