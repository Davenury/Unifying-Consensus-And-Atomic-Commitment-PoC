package com.github.davenury.ucac.utils

import org.slf4j.MDC

/**
 * @author Kamil Jarosz
 */
class MdcProvider(val mdc: Map<String, String>) {
    inline fun <R> withMdc(action: () -> R): R {
        val oldMdc = MDC.getCopyOfContextMap() ?: HashMap()
        mdc.forEach { MDC.put(it.key, it.value) }
        try {
            return action()
        } finally {
            MDC.setContextMap(oldMdc)
        }
    }
}
