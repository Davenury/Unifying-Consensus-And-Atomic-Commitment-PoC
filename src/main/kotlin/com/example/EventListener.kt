typealias AdditionalActionConsensus = suspend () -> Unit

enum class ConsensusTestAddon {
    BeforeSendingElect,
    AfterProposingChange
}