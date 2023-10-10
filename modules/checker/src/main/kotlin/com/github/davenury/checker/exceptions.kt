package com.github.davenury.checker

enum class ChangesArentTheSameReason {
    DIFFERENT_SIZES, DIFFERENT_CHANGES, DIFFERENT_CHANGES_BETWEEN_PEERSETS
}

class ChangesArentTheSameException(reason: ChangesArentTheSameReason):
    Exception("Changes are not the same in single peerset - reason: ${reason.name.lowercase()}")