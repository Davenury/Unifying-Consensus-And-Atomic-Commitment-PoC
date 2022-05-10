package com.example.domain

class MissingParameterException(message: String?) : Exception(message)
class UnknownOperationException(val desiredOperationName: String) : Exception()

@kotlinx.serialization.Serializable
data class ErrorMessage(val msg: String)