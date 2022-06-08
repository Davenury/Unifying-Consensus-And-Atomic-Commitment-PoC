package com.example.domain

interface ProtocolTimer {
    fun startCounting(action: suspend () -> Unit)
    fun cancelCounting()
}