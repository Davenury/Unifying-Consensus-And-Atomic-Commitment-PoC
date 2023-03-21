package com.github.davenury.ucac.commitment.gpac

enum class GPACPhase {
    ELECT {
        override fun shouldExecutePhaseIGotNow(phaseToExecute: GPACPhase) = true
    }, AGREE {
        override fun shouldExecutePhaseIGotNow(phaseToExecute: GPACPhase) =
            true
    }, APPLY {
        override fun shouldExecutePhaseIGotNow(phaseToExecute: GPACPhase) = phaseToExecute != AGREE
    };

    abstract fun shouldExecutePhaseIGotNow(phaseToExecute: GPACPhase): Boolean
}