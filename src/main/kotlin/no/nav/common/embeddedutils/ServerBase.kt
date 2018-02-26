package no.nav.common.embeddedutils

abstract class ServerBase {

    val host: String = "localhost"
    abstract val port: Int
    abstract val url: String

    abstract fun start()
    abstract fun stop()
}