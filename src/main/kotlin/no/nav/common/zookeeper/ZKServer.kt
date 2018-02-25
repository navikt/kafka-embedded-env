package no.nav.common.zookeeper

import no.nav.common.utils.*
import org.apache.commons.io.FileUtils
import org.apache.zookeeper.server.ServerCnxnFactory
import org.apache.zookeeper.server.ServerConfig
import org.apache.zookeeper.server.ZooKeeperServer
import org.apache.zookeeper.server.persistence.FileTxnSnapLog
import java.io.File

class ZKServer private constructor(override val port: Int) : ServerBase() {

    override val url = "$host:$port"

    private val dataDir = File(System.getProperty("java.io.tmpdir"), "inmzookeeper").apply {
        // in case of fatal failure and no deletion in previous run
        FileUtils.deleteDirectory(this)
    }
    private val args = arrayOf(port.toString(), dataDir.absolutePath, "3000", "0")
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
        cnxnFactory.shutdown() // includes shut down of zookeeper server
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

        override fun getHost(): String = servers.firstOrNull()?.host ?: ""

        override fun getPort(): Int = servers.firstOrNull()?.port ?: 0

        override fun getUrl(): String = servers.firstOrNull()?.url ?: ""
    }
}