package no.nav.common.embeddedutils

import java.net.ServerSocket
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.UUID

/**
 * A function returning the next available socket port
 */
fun getAvailablePort(): Int = ServerSocket(0).run {
    reuseAddress = true
    close()
    localPort
}

fun deleteDir(dir: Path) {
    if (Files.exists(dir)) {
        Files.walk(dir).sorted(Comparator.reverseOrder()).forEach { Files.delete(it) }
    }
}

private val tmpDir = Paths.get(System.getProperty("java.io.tmpdir"))

fun appDirFor(appName: String): Path = tmpDir.resolve(appName)

fun dataDirFor(path: Path): Path = path.resolve(UUID.randomUUID().toString()).apply {
    Files.createDirectories(this)
}
