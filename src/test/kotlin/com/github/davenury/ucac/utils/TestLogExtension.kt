package com.github.davenury.ucac.utils

import io.mockk.every
import io.mockk.mockkStatic
import org.fusesource.jansi.AnsiConsole
import org.junit.jupiter.api.extension.AfterEachCallback
import org.junit.jupiter.api.extension.BeforeEachCallback
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.api.extension.ExtensionContext.Store.CloseableResource
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import java.math.BigInteger
import java.security.MessageDigest

/**
 * @author Kamil Jarosz
 */
class TestLogExtension : BeforeEachCallback, AfterEachCallback, CloseableResource {
    companion object {
        val logger: Logger = LoggerFactory.getLogger("test logging")
    }

    private fun hash(className: String?, methodName: String?): String {
        val digest = MessageDigest.getInstance("SHA-256").digest("$className:$methodName".toByteArray())
        return BigInteger(1, digest).toString(16).substring(0, 8)
    }

    override fun beforeEach(context: ExtensionContext) {
        disableAnsiConsoleInstallation();

        val className = context.testClass.orElse(null)?.name
        val methodName = context.testMethod.orElse(null)?.name
        val hash = hash(className, methodName)
        MDC.put("test", hash)
        logger.info("===== Start test: $className/$methodName =====")
    }

    override fun afterEach(context: ExtensionContext) {
        val className = context.testClass.orElse(null)?.name
        val methodName = context.testMethod.orElse(null)?.name
        logger.info("===== End test: $className/$methodName =====")
        MDC.remove("test")
    }

    override fun close() {

    }

    private fun disableAnsiConsoleInstallation() {
        mockkStatic(AnsiConsole::class)
        every {
            AnsiConsole.systemInstall()
        } throws IllegalStateException("Ansi console not supported in tests")
    }
}
