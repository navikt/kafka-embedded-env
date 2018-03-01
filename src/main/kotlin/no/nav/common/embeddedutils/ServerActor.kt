package no.nav.common.embeddedutils

/**
 * An abstract class for a servers companion object, managing the server
 */
abstract class ServerActor<ServerBase> {

    protected val servers = mutableListOf<ServerBase>()

    abstract fun onReceive(msg: ServerMessages)

    abstract fun getHost(): String

    abstract fun getPort(): Int

    abstract fun getUrl(): String
}