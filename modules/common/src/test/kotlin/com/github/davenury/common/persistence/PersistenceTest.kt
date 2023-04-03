package com.github.davenury.common.persistence

import org.junit.jupiter.api.*

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.testcontainers.containers.GenericContainer
import org.testcontainers.utility.DockerImageName
import strikt.api.expectThat
import strikt.assertions.isEqualTo
import strikt.assertions.isNull
import java.util.stream.Stream

/**
 * @author Kamil Jarosz
 */
class PersistenceTest {
    @ParameterizedTest
    @MethodSource("implementations")
    fun getSet(persistence: Persistence) {
        expectThat(persistence.get("nonexistentkey")).isNull()
        expectThat(persistence.get("key")).isNull()

        persistence.set("key", "value")
        expectThat(persistence.get("key")).isEqualTo("value")
        persistence.set("key", "value2")
        expectThat(persistence.get("key")).isEqualTo("value2")
    }

    @ParameterizedTest
    @MethodSource("implementations")
    fun compareAndExchange(persistence: Persistence) {
        expectThat(persistence.compareAndExchange("key2", "X", "Y")).isNull()
        expectThat(persistence.get("key2")).isNull()
        persistence.set("key2", "value0")

        expectThat(persistence.compareAndExchange("key2", "value0", "value1")).isEqualTo("value0")
        expectThat(persistence.compareAndExchange("key2", "value0", "value2")).isEqualTo("value1")
        expectThat(persistence.get("key2")).isEqualTo("value1")
        expectThat(persistence.compareAndExchange("key2", "value1", "value2")).isEqualTo("value1")
        expectThat(persistence.get("key2")).isEqualTo("value2")
        expectThat(persistence.compareAndExchange("key2", "value2", "value0")).isEqualTo("value2")
        expectThat(persistence.get("key2")).isEqualTo("value0")
    }

    companion object {
        private lateinit var redis: GenericContainer<*>

        @BeforeAll
        @JvmStatic
        fun setUp() {
            redis = GenericContainer(DockerImageName.parse("redis:7.0-alpine"))
                .withExposedPorts(6379)
            redis.start()
        }

        @AfterAll
        @JvmStatic
        fun tearDown() {
            redis.stop()
        }

        @JvmStatic
        fun implementations(): Stream<Arguments> {
            return Stream.of(
                Arguments.of(InMemoryPersistence()),
                Arguments.of(RedisPersistence(redis.host, redis.getMappedPort(6379))),
            )
        }
    }
}
