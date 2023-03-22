package com.github.davenury.ucac.commitment.gpac

enum class GPACPhase {
    ELECT {
        override fun shouldExecutePhaseIGotNow(phaseToExecute: GPACPhase) = true
    }, AGREE {
        override fun shouldExecutePhaseIGotNow(phaseToExecute: GPACPhase) = phaseToExecute == APPLY
    }, APPLY {
        override fun shouldExecutePhaseIGotNow(phaseToExecute: GPACPhase) = false
    };

    abstract fun shouldExecutePhaseIGotNow(phaseToExecute: GPACPhase): Boolean
}