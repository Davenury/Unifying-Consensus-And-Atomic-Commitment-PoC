package com.example

import com.example.domain.Transaction

typealias AdditionalAction = (Transaction?) -> Unit

enum class TestAddon {
    OnSendingElect,
    OnHandlingElectBegin,
    OnHandlingElectEnd,
    OnSendingAgree,
    OnHandlingAgreeBegin,
    OnHandlingAgreeEnd,
    OnSendingApply,
    OnHandlingApplyBegin,
    OnHandlingApplyEnd
}