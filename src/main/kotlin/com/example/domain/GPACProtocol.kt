package com.example.domain

data class State(val ballotNumber: Int, val initVal: Accept, val acceptNum: Int, val acceptVal: Accept?, val decision: Boolean) {
    companion object {
        fun defaultState() =
            State(0, Accept.ABORT, 0, null, false)

        fun resetStateAfterTransaction(prevState: State) =
            State(prevState.ballotNumber, Accept.ABORT, 0, null, false)
    }
}

interface GPACProtocol {
    fun handleElect(message: ElectMe): ElectedYou
    fun handleAgree(message: Agree): Agreed
    fun handleApply(message: Apply)
    fun performProtocolAsLeader(change: ChangeDto)
    fun getState(): State
}

class GPACProtocolImpl(
    private val historyManagement: HistoryManagement
): GPACProtocol {
    private var state = State.defaultState()

    private fun checkBallotNumber(ballotNumber: Int): Boolean =
        ballotNumber > state.ballotNumber

    override fun getState(): State = this.state

    override fun handleElect(message: ElectMe): ElectedYou {
        if (!this.checkBallotNumber(message.ballotNumber)) throw NotElectingYou(this.state.ballotNumber)
        val initVal = if(historyManagement.canBeBuild(message.change.toChange())) Accept.COMMIT else Accept.ABORT

        return ElectedYou(state.ballotNumber, initVal, state.acceptNum, state.acceptVal, state.decision)
    }

    override fun handleAgree(message: Agree): Agreed {
        if(checkBallotNumber(message.ballotNumber)) {
            val acceptVal = if(historyManagement.canBeBuild(message.change.toChange())) Accept.COMMIT else Accept.ABORT
            this.state = this.state.copy(ballotNumber = message.ballotNumber, acceptVal = acceptVal)

            // TODO: lock?

            return Agreed(state.ballotNumber, acceptVal)
        }
        throw NotElectingYou(this.state.ballotNumber)
    }

    override fun handleApply(message: Apply) {
        this.state = this.state.copy(decision = true, acceptVal = Accept.COMMIT)
        historyManagement.change(message.change.toChange())
        this.state = State.resetStateAfterTransaction(this.state)
    }

    override fun performProtocolAsLeader(change: ChangeDto) {
        TODO("Not yet implemented")
    }
}