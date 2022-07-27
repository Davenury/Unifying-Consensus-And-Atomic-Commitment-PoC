package github.davenury.ucac.common

class MissingParameterException(message: String?) : Exception(message)
class UnknownOperationException(val desiredOperationName: String) : Exception()
class NotElectingYou(val ballotNumber: Int, val messageBallotNumber: Int): Exception()
class NotValidLeader(val ballotNumber: Int, val messageBallotNumber: Int): Exception()
class MaxTriesExceededException: Exception()
class TooFewResponsesException: Exception()
class HistoryCannotBeBuildException: Exception()
class AlreadyLockedException: Exception()

data class ErrorMessage(val msg: String)