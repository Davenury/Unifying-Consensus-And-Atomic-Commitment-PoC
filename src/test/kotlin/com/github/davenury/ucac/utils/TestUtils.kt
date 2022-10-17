package com.github.davenury.ucac.utils

import java.lang.AssertionError
import java.nio.file.Path
import java.nio.file.Paths
import kotlin.io.path.absolute
import kotlin.io.path.toPath

/**
 * @author Kamil Jarosz
 */
object TestUtils {
    fun getResource(name: String): Path {
        val resource = javaClass.classLoader.getResource(name)
            ?: throw AssertionError("Resource $name not found")

        return resource.toURI().toPath()
    }

    fun getRepoRoot(): Path {
        return Paths.get(".").absolute()
    }
}
