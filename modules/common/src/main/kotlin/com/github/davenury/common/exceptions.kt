package com.github.davenury.common

import com.github.davenury.common.txblocker.TransactionAcquisition
import java.util.*

class MissingParameterException(message: String?) : Exception(message)
class UnknownOperationException(val desiredOperationName: String) : Exception()
class NotElectingYou(val ballotNumber: Int, val messageBallotNumber: Int) : Exception()
class NotValidLeader(val ballotNumber: Int, val messageBallotNumber: Int) : Exception()
class HistoryCannotBeBuildException : Exception()

class AlreadyLockedException(acquisition: TransactionAcquisition) : Exception(
    "We cannot perform your transaction, as another transaction is currently running: $acquisition"
)
class NotLockedException(acquisition: TransactionAcquisition) : Exception(
    "Tried to release an unacquired lock by $acquisition"
)

class TransactionNotBlockedOnThisChange(protocol: ProtocolName, changeId: String) :
    Exception("Protocol ${protocol.name.lowercase()} is not blocked on this change: $changeId")

class ChangeDoesntExist(changeId: String) : Exception("Change with id: $changeId doesn't exists")
class TwoPCConflictException(msg: String) : Exception("During 2PC occurs error: $msg")
class TwoPCHandleException(msg: String) : Exception("In 2PC occurs error: $msg")
class GPACInstanceNotFoundException(changeId: String) : Exception("GPAC instance for change $changeId wasn't found!")
class AlvinLeaderBecameOutdatedException(changeId: String) : Exception("I as a leader become outdated for entry $changeId")
class AlvinOutdatedPrepareException(prevEpoch: Int, currEpoch: Int) : Exception("Receive prepare from previous epoch $prevEpoch, current: $currEpoch")
class AlvinHistoryBlocked(changeId: String, protocol: ProtocolName) : Exception("TransactionBlocker is blocked on change: $changeId with protocol $protocol")

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
