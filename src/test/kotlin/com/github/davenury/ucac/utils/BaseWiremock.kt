package com.github.davenury.ucac.utils

import com.github.davenury.ucac.commitment.gpac.Accept
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.*
import org.junit.jupiter.api.AfterAll

abstract class BaseWiremock {
    private var wireMockServer: WireMockServer = WireMockServer(0)

    init {
        wireMockServer.start()
    }

    fun stubForElectMe(ballotNumber: Int, initVal: Accept, acceptNum: Int, acceptVal: Accept?, decision: Boolean) {
        wireMockServer.stubFor(post("/elect")
            .willReturn(
                aResponse().withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody("""{"ballotNumber":$ballotNumber,"initVal":"$initVal","acceptNum":$acceptNum,"acceptVal":${acceptVal?.let { "$acceptVal" }},"decision":$decision}""")
            ))
    }

    fun stubForNotElectingYou() {
        wireMockServer.stubFor(
            post("/elect")
                .willReturn(
                    aResponse().withStatus(422)
                )
        )
    }

    fun stubForAgree(ballotNumber: Int, acceptVal: Accept) {
        wireMockServer.stubFor(
            post("/ft-agree")
                .willReturn(
                    aResponse().withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("""{"ballotNumber":$ballotNumber,"acceptVal":"$acceptVal"}""")
                )
        )
    }

    fun stubForNotAgree() {
        wireMockServer.stubFor(
            post("/ft-agree")
                .willReturn(
                    aResponse().withStatus(422)
                )
        )
    }

    fun stubForApply() {
        wireMockServer.stubFor(
            post("/apply")
                .willReturn(
                    aResponse().withStatus(200)
                )
        )
    }

    fun verifyMaxRetriesForElectionPassed(maxRetries: Int) {
        wireMockServer.verify(maxRetries, postRequestedFor(urlMatching("/elect")))
    }

    fun verifyAgreeStub(expected: Int) {
        wireMockServer.verify(expected, postRequestedFor(urlMatching("/ft-agree")))
    }

    fun getPort(): Int = wireMockServer.port()
}
