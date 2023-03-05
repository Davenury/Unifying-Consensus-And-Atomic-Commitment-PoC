package com.github.davenury.ucac.utils

import java.nio.file.Path
import java.nio.file.Paths
import kotlin.io.path.absolute

/**
 * @author Kamil Jarosz
 */
object TestUtils {
    fun getRepoRoot(): Path = Paths.get(".").absolute()
}
