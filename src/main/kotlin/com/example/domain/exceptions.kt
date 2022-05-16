package com.example.domain

class MissingParameterException(message: String?) : Exception(message)
class UnknownOperationException(val desiredOperationName: String) : Exception()
class NotElectingYou(val ballotNumber: Int): Exception()
class MaxTriesExceededException: Exception()
class TooFewResponsesException: Exception()
class HistoryCannotBeBuildException: Exception()

data class ErrorMessage(val msg: String)