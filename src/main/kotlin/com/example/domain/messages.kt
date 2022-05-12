package com.example.domain

enum class Accept {
    COMMIT,
    ABORT;
}
data class ElectMe(val ballotNumber: Int, val change: ChangeDto)
data class ElectedYou(val ballotNumber: Int, val initVal: Accept, val acceptNum: Int, val acceptVal: Accept?, val decision: Boolean)
data class Agree(val ballotNumber: Int, val acceptVal: Accept, val change: ChangeDto)
data class Agreed(val ballotNumber: Int, val acceptVal: Accept)
data class Apply(val ballotNumber: Int, val decision: Boolean, val change: ChangeDto)
