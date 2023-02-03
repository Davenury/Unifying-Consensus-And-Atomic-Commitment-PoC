package com.github.davenury.tests.strategies.changes

import com.github.davenury.common.Change
import com.github.davenury.tests.OnePeersetChanges

interface CreateChangeStrategy {
    fun createChange(ids: List<Int>, changes: Map<Int, OnePeersetChanges>): Change
}