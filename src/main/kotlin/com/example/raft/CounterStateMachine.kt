package com.example.raft

import java.util.concurrent.atomic.AtomicInteger


/**
 * State machine implementation for Counter server application. This class
 * maintain a [AtomicInteger] object as a state and accept two commands:
 * GET and INCREMENT, GET is a ReadOnly command which will be handled by
 * `query` method however INCREMENT is a transactional command which
 * will be handled by `applyTransaction`.
 */
class CounterStateMachine(
    private var counter: AtomicInteger = AtomicInteger(0)
) : StateMachine<AtomicInteger>(counter) {

    override fun applyOperation(operation: String): String? {
        return if (operation == "INCREMENT") {
            counter.incrementAndGet()
            null
        } else
            "Invalid Command"
    }

    override fun queryOperation(operation: String): String {
        return if (operation != "GET") "Invalid Command" else counter.toString()
    }

    override fun serializeState(): String = state.toString()

}