package com.github.davenury.common

import com.sksamuel.hoplite.ConfigLoaderBuilder
import com.sksamuel.hoplite.MapPropertySource
import com.sksamuel.hoplite.addResourceSource
import com.sksamuel.hoplite.decoder.Decoder

fun parsePeers(peers: String): List<List<String>> {
    return peers.split(";")
        .map { peerset -> peerset.split(",").map { it.trim() } }
}


inline fun <reified T>loadConfig(overrides: Map<String, Any> = emptyMap(), decoders: List<Decoder<*>> = listOf()): T {
    val configFile = System.getProperty("configFile")
        ?: System.getenv("CONFIG_FILE")
        ?: "application.conf"
    return loadConfig(configFile, overrides, decoders = decoders)
}

inline fun <reified T>loadConfig(configFileName: String, overrides: Map<String, Any> = emptyMap(), decoders: List<Decoder<*>> = listOf()): T =
    ConfigLoaderBuilder
        .default()
        .addSource(MapPropertySource(overrides))
        .addResourceSource("/$configFileName")
        .addDecoders(decoders)
        .build()
        .loadConfigOrThrow()
