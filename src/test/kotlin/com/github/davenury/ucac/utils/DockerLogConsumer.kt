package com.github.davenury.ucac.utils

import org.slf4j.LoggerFactory
import org.testcontainers.containers.output.OutputFrame
import java.nio.charset.StandardCharsets
import java.util.function.Consumer

class DockerLogConsumer(private val name: String) : Consumer<OutputFrame> {
    override fun accept(t: OutputFrame) {
        when (t.type!!) {
            OutputFrame.OutputType.STDOUT -> {
                val msg = getMessage(t.bytes)
                println("[container/$name/stdout] $msg")
            }

            OutputFrame.OutputType.STDERR -> {
                val msg = getMessage(t.bytes)
                println("[container/$name/stderr] $msg")
            }

            OutputFrame.OutputType.END -> {
                println("[container/$name/end]")
            }
        }
    }

    private fun getMessage(bytes: ByteArray?): Any {
        val msg = String(bytes ?: ByteArray(0), StandardCharsets.UTF_8)
        if (msg.endsWith("\r\n")) return msg.substring(0, msg.length - 2)
        if (msg.endsWith("\n")) return msg.substring(0, msg.length - 1)
        return msg
    }
}
