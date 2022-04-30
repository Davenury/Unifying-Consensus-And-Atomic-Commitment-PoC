package com.example.domain


@kotlinx.serialization.Serializable
data class Change(
    val from: String,
    val to: String,
    val operation: String
)
