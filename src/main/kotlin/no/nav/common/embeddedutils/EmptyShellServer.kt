package no.nav.common.embeddedutils

class EmptyShellServer : ServerBase() {

    override val host = ""
    override val port = 0
    override val url = ""

    override fun start() {}
    override fun stop() {}
}