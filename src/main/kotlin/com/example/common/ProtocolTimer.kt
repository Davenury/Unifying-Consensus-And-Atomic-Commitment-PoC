package com.example.common

interface ProtocolTimer {
    suspend fun startCounting(action: suspend () -> Unit)
    fun cancelCounting()
}