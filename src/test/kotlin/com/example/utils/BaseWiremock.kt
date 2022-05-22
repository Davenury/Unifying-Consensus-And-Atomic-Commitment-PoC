package com.example.utils

import com.example.domain.Accept
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.*
import org.junit.jupiter.api.AfterAll

abstract class BaseWiremock(
    PORT: Int
) {

    private var wireMockServer: WireMockServer = WireMockServer(PORT)

    init {
        wireMockServer.start()
    }

    @AfterAll
    fun cleanup() {
        wireMockServer.resetAll()
    }

    fun stubForElectMe(ballotNumber: Int, initVal: Accept, acceptNum: Int, acceptVal: Accept?, decision: Boolean) {
        wireMockServer.stubFor(post("/elect")
            .willReturn(
                aResponse().withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody("""{"ballotNumber":$ballotNumber,"initVal":"$initVal","acceptNum":$acceptNum,"acceptVal":${acceptVal?.let { "$acceptVal" } ?: "null"},"decision":$decision}""")
            ))
    }

    fun stubForNotElectingYou() {
        wireMockServer.stubFor(post("/elect")
            .willReturn(
                aResponse().withStatus(422)
            ))
    }

    fun stubForAgree(ballotNumber: Int, acceptVal: Accept) {
        wireMockServer.stubFor(post("/ft-agree")
            .willReturn(
                aResponse().withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody("""{"ballotNumber":$ballotNumber,"acceptVal":"$acceptVal"}""")
            ))
    }

    fun stubForNotAgree() {
        wireMockServer.stubFor(post("/ft-agree")
            .willReturn(
                aResponse().withStatus(422)
            ))
    }

    fun stubForApply() {
        wireMockServer.stubFor(post("/apply")
            .willReturn(
                aResponse().withStatus(200)
            ))
    }

    fun verifyMaxRetriesForElectionPassed(maxRetries: Int) {
        wireMockServer.verify(maxRetries, postRequestedFor(urlMatching("/elect")))
    }

    fun verifyAgreeStub(expected: Int) {
        wireMockServer.verify(expected, postRequestedFor(urlMatching("/ft-agree")))
    }

    fun verifyApplyStub(expected: Int) {
        wireMockServer.verify(expected, postRequestedFor(urlMatching("/apply")))
    }

}