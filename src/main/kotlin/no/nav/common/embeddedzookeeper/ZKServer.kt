package no.nav.common.embeddedzookeeper

import no.nav.common.embeddedutils.*
import org.apache.zookeeper.server.ServerCnxnFactory
import org.apache.zookeeper.server.ServerConfig
import org.apache.zookeeper.server.ZooKeeperServer
import org.apache.zookeeper.server.persistence.FileTxnSnapLog
import java.io.File

class ZKServer(override val port: Int, private val dataDir: File) : ServerBase() {

    // see link below for starting up embeddedzookeeper...
    // https://github.com/apache/zookeeper/blob/e0af6ed7598fc4555d7625ddc8efd86e2281babf/src/java/main/org/apache/zookeeper/server/ZooKeeperServerMain.java

    override val url = "$host:$port"

    // not possible to stop and restart zookeeper, must use core inner class
    private class ZKS(port: Int, dataDir: File) {

        private val args = arrayOf(port.toString(), dataDir.absolutePath, "1000", "0")
        private val config = ServerConfig().apply { parse(args) }

        //private val shutdownLatch = CountDownLatch(1) - cannot set shutDownHandler

        val txnLog = FileTxnSnapLog(File(config.dataLogDir), File(config.dataDir))

        val zkServer = ZooKeeperServer().apply {
            txnLogFactory = txnLog
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
            ZKS(port,dataDir).apply {
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
                txnLog.close()
            }
            zk.removeAll { true }
            status = NotRunning
        }
        else -> {}
    }
}