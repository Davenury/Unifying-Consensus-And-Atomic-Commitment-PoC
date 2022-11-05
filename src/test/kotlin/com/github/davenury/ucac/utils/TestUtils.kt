package com.github.davenury.ucac.utils

import java.nio.file.Path
import java.nio.file.Paths
import kotlin.io.path.absolute
import kotlin.io.path.toPath

/**
 * @author Kamil Jarosz
 */
object TestUtils {
    fun getResource(name: String): Path = javaClass.classLoader.getResource(name)
        ?.toURI()
        ?.toPath()
        ?: throw AssertionError("Resource $name not found")

    fun getRepoRoot(): Path = Paths.get(".").absolute()
}
