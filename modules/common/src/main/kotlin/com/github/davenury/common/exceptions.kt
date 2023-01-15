package com.github.davenury.common

class MissingParameterException(message: String?) : Exception(message)
class UnknownOperationException(val desiredOperationName: String) : Exception()
class NotElectingYou(val ballotNumber: Int, val messageBallotNumber: Int) : Exception()
class NotValidLeader(val ballotNumber: Int, val messageBallotNumber: Int) : Exception()
class HistoryCannotBeBuildException : Exception()
class AlreadyLockedException : Exception()
class ChangeDoesntExist(changeId: String): Exception("Change with id: $changeId doesn't exists")
class TwoPCConflictException(msg: String): Exception("During 2PC occurs error: $msg")
class TwoPCHandleException(msg: String): Exception("In 2PC occurs error: $msg")
class GPACInstanceNotFoundException(changeId: String): Exception("GPAC instance for change $changeId wasn't found!")

data class ErrorMessage(val msg: String)
enum class ChangeCreationStatus {
    APPLIED,
    NOT_APPLIED,
    UNKNOWN,
}

data class ChangeCreationResponse(
    val message: String,
    val detailedMessage: String?,
    val changeStatus: ChangeCreationStatus,
)
