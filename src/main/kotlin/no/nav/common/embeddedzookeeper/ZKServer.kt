package no.nav.common.embeddedzookeeper

import no.nav.common.embeddedutils.ServerBase
import no.nav.common.embeddedutils.Running
import no.nav.common.embeddedutils.NotRunning
import org.apache.zookeeper.server.ServerCnxnFactory
import org.apache.zookeeper.server.ServerConfig
import org.apache.zookeeper.server.ZooKeeperServer
import org.apache.zookeeper.server.persistence.FileTxnSnapLog
import java.io.File

class ZKServer(override val port: Int, private val dataDir: File) : ServerBase() {

    // see link for ZooKeeperServerMain below for starting up embeddedzookeeper
    // https://github.com/apache/zookeeper/blob/branch-3.4.13/src/java/main/org/apache/zookeeper/server/ZooKeeperServerMain.java

    override val url = "$host:$port"

    // not possible to stop and restart zookeeper, must use core inner class
    private class ZKS(port: Int, dataDir: File) {

        private val args = arrayOf(port.toString(), dataDir.absolutePath, "1000", "0")
        private val config = ServerConfig().apply { parse(args) }

        val zkServer = ZooKeeperServer().apply {

            // private val shutdownLatch = CountDownLatch(1) - cannot set shutDownHandler for this instance
            txnLogFactory = FileTxnSnapLog(File(config.dataLogDir), File(config.dataDir))
            tickTime = config.tickTime
            minSessionTimeout = config.minSessionTimeout
            maxSessionTimeout = config.maxSessionTimeout
        }

        val cnxnFactory: ServerCnxnFactory = ServerCnxnFactory.createFactory().apply {
            configure(config.clientPortAddress, config.maxClientCnxns)
        }
    }

    private val zk = mutableListOf<ZKS>()

    override fun start() = when (status) {
        NotRunning -> {
            ZKS(port, dataDir).apply {
                zk.add(this)
                cnxnFactory.startup(zkServer)
            }
            status = Running
        }
        else -> {}
    }

    override fun stop() = when (status) {
        Running -> {
            zk.first().apply {
                cnxnFactory.shutdown() // includes shut down of embeddedzookeeper server
                cnxnFactory.join()
                zkServer.txnLogFactory.close()
            }
            zk.removeAll { true }
            status = NotRunning
        }
        else -> {}
    }
}