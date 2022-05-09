package com.example

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import java.net.http.HttpClient

val objectMapper = jacksonObjectMapper()
val client = HttpClient.newBuilder().build()