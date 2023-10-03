package no.nav.common.embeddedutils

/**
 * An abstract class for a server
 * Start and stop, nothing more to it
 */
abstract class ServerBase {
    protected var status: ServerStatus = ServerStatus.NotRunning

    open val host: String = "localhost"
    abstract val port: Int
    abstract val url: String

    abstract fun start()

    abstract fun stop()
}
