package no.nav.common.utils

abstract class ServerActor<ServerBase> {

    protected val servers = mutableListOf<ServerBase>()

    abstract fun onReceive(msg: ServerMessages)

    abstract fun getHost(): String

    abstract fun getPort(): Int

    abstract fun getUrl(): String
}