package no.nav.common.embeddedzookeeper

import no.nav.common.embeddedutils.*
import org.apache.commons.io.FileUtils
import org.apache.zookeeper.server.ServerCnxnFactory
import org.apache.zookeeper.server.ServerConfig
import org.apache.zookeeper.server.ZooKeeperServer
import org.apache.zookeeper.server.persistence.FileTxnSnapLog
import java.io.File

class ZKServer private constructor(override val port: Int) : ServerBase() {

    // see link below for starting up embeddedzookeeper...
    // https://github.com/apache/zookeeper/blob/e0af6ed7598fc4555d7625ddc8efd86e2281babf/src/java/main/org/apache/zookeeper/server/ZooKeeperServerMain.java

    override val url = "$host:$port"

    private val dataDir = File(System.getProperty("java.io.tmpdir"), "inmzookeeper").apply {
        // in case of fatal failure and no deletion in previous run
        FileUtils.deleteDirectory(this)
    }
    private val args = arrayOf(port.toString(), dataDir.absolutePath, "1000", "0")
    private val config = ServerConfig().apply { parse(args) }

    //private val shutdownLatch = CountDownLatch(1) - cannot set shutDownHandler

    private val txnLog = FileTxnSnapLog(File(config.dataLogDir), File(config.dataDir))

    private val zkServer = ZooKeeperServer().apply {
        txnLogFactory = txnLog
        tickTime = config.tickTime
        minSessionTimeout = config.minSessionTimeout
        maxSessionTimeout = config.maxSessionTimeout
    }

    private val cnxnFactory = ServerCnxnFactory.createFactory().apply {
        configure(config.clientPortAddress, config.maxClientCnxns)
    }

    override fun start() = cnxnFactory.startup(zkServer)

    override fun stop() {
        cnxnFactory.shutdown() // includes shut down of embeddedzookeeper server
        cnxnFactory.join()
        txnLog.close()
        FileUtils.deleteDirectory(dataDir)
    }

    companion object : ServerActor<ZKServer>() {

        override fun onReceive(msg: ServerMessages) {

            when (msg) {
                ZKStart -> if (servers.isEmpty()) {
                        ZKServer(getAvailablePort()).run {
                            servers.add(this)
                            start()
                        }
                    }

                ZKStop -> if (!servers.isEmpty()) {
                    servers.first().stop()
                    servers.removeAt(0)
                    }

                else -> {
                    // don't care about other messages
                }
            }

        }

        override fun getHost() = servers.firstOrNull()?.host ?: ""

        override fun getPort() = servers.firstOrNull()?.port ?: 0

        override fun getUrl() = servers.firstOrNull()?.url ?: ""
    }
}