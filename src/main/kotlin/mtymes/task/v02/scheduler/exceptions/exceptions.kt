package mtymes.task.v02.scheduler.exceptions

class ExecutionNotFoundException(message: String) : IllegalStateException(message)
class NotLastExecutionException(message: String) : IllegalStateException(message)
class UnexpectedStatusException(message: String) : IllegalStateException(message)
class UnknownFailureReasonException(message: String) : IllegalStateException(message)

class ExecutionSupersededByAnotherOneException(message: String) : IllegalStateException(message)
