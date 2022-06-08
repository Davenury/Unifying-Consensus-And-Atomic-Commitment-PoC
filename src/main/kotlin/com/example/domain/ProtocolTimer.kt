package com.example.domain

interface ProtocolTimer {
    fun startCounting(fn: () -> Unit)
    fun cancelCounting()
}