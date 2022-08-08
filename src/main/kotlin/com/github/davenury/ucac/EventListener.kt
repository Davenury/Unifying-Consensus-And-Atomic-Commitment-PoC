
package com.github.davenury.ucac

import com.github.davenury.ucac.gpac.domain.Transaction


typealias AdditionalActionConsensus = suspend () -> Unit

enum class ConsensusTestAddon {
    BeforeSendingElect,
    AfterProposingChange
}
