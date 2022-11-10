package com.github.davenury.ucac.common

class MissingParameterException(message: String?) : Exception(message)
class UnknownOperationException(val desiredOperationName: String) : Exception()
class NotElectingYou(val ballotNumber: Int, val messageBallotNumber: Int) : Exception()
class NotValidLeader(val ballotNumber: Int, val messageBallotNumber: Int) : Exception()
class MaxTriesExceededException : Exception()
class TooFewResponsesException : Exception()
class HistoryCannotBeBuildException : Exception()
class AlreadyLockedException : Exception()
class ChangeDoesntExist(changeId: String): Exception()
class PeerNotInPeersetException(peer: String): Exception("Peer $peer is not found in any of config peersets!")
data class ErrorMessage(val msg: String)
data class ChangeCreationErrorMessage(
    val msg: String,
    val changeApplied: String,
)


