package com.github.davenury.common.history

import org.junit.jupiter.api.Test
import strikt.api.expectThat
import strikt.api.expectThrows
import strikt.assertions.isEqualTo
import strikt.assertions.isFalse
import strikt.assertions.isTrue

/**
 * @author Kamil Jarosz
 */
class EntryIdTest {
    @Test
    fun testFromString() {
        EntryId.fromString("27c74670adb75075fad058d5ceaf7b20c4e7786c83bae8a32f626f9782af34c9a33c2046ef60fd2a7878d378e29fec851806bbd9a67878f3a9f1cda4830763fd")
        EntryId.fromString("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")

        expectThrows<IllegalArgumentException> {
            EntryId.fromString("")
        }
        expectThrows<IllegalArgumentException> {
            // too short
            EntryId.fromString("27c74670adb75075fad058d5ceaf7b20c4e7786c83bae8a32f626f9782af34c9a33c2046ef60fd2a7878d378e29fec851806bbd9a67878f3a9f1cda4830763f")
        }
        expectThrows<IllegalArgumentException> {
            // too long
            EntryId.fromString("27c74670adb75075fad058d5ceaf7b20c4e7786c83bae8a32f626f9782af34c9a33c2046ef60fd2a7878d378e29fec851806bbd9a67878f3a9f1cda4830763fdd")
        }
        expectThrows<IllegalArgumentException> {
            // an illegal radix 16 digit — h
            EntryId.fromString("h7c74670adb75075fad058d5ceaf7b20c4e7786c83bae8a32f626f9782af34c9a33c2046ef60fd2a7878d378e29fec851806bbd9a67878f3a9f1cda4830763fd")
        }
    }

    @Test
    fun testEqualsAndHashCode() {
        val e1 =
            EntryId.fromString("27c74670adb75075fad058d5ceaf7b20c4e7786c83bae8a32f626f9782af34c9a33c2046ef60fd2a7878d378e29fec851806bbd9a67878f3a9f1cda4830763fd")
        val e2 =
            EntryId.fromString("27c74670adb75075fad058d5ceaf7b20c4e7786c83bae8a32f626f9782af34c9a33c2046ef60fd2a7878d378e29fec851806bbd9a67878f3a9f1cda4830763fd")
        val e3 =
            EntryId.fromString("22c74670adb75075fad058d5ceaf7b20c4e7786c83bae8a32f626f9782af34c9a33c2046ef60fd2a7878d378e29fec851806bbd9a67878f3a9f1cda4830763fd")

        expectThat(e1 == e2).isTrue()
        expectThat(e2 == e1).isTrue()
        expectThat(e1 == e3).isFalse()
        expectThat(e2 == e3).isFalse()
        expectThat(e3 == e1).isFalse()
        expectThat(e3 == e2).isFalse()

        expectThat(e1.hashCode()).isEqualTo(e2.hashCode())
    }

    @Test
    fun testToString() {
        val e =
            EntryId.fromString("27c74670adb75075fad058d5ceaf7b20c4e7786c83bae8a32f626f9782af34c9a33c2046ef60fd2a7878d378e29fec851806bbd9a67878f3a9f1cda4830763fd")

        expectThat(e.toString()).isEqualTo("27c74670adb75075fad058d5ceaf7b20c4e7786c83bae8a32f626f9782af34c9a33c2046ef60fd2a7878d378e29fec851806bbd9a67878f3a9f1cda4830763fd")
    }
}
