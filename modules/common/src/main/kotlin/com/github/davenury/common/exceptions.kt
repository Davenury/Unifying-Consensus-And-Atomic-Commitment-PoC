package com.github.davenury.common

class MissingParameterException(message: String?) : Exception(message)
class UnknownOperationException(val desiredOperationName: String) : Exception()
class NotElectingYou(val ballotNumber: Int, val messageBallotNumber: Int) : Exception()
class NotValidLeader(val ballotNumber: Int, val messageBallotNumber: Int) : Exception()
class HistoryCannotBeBuildException : Exception()
class AlreadyLockedException : Exception()
class ChangeDoesntExist(changeId: String): Exception("Change with id: $changeId doesn't exists")
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
