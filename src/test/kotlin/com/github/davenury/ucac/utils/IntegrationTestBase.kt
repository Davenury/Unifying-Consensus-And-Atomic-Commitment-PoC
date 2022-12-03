package com.github.davenury.ucac.utils

import org.junit.jupiter.api.AfterEach

/**
 * @author Kamil Jarosz
 */
abstract class IntegrationTestBase {
    lateinit var apps: TestApplicationSet

    @AfterEach
    internal fun tearDown() {
        if (this::apps.isInitialized) {
            apps.stopApps()
        }
    }
}
