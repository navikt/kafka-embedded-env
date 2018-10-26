package no.nav.common.embeddedzookeeper

import no.nav.common.embeddedutils.ServerBase
import no.nav.common.embeddedutils.ServerStatus
import org.apache.commons.io.FileUtils
import org.apache.zookeeper.server.ZooKeeperServerMain
import java.io.BufferedReader
import java.io.File
import java.io.FileOutputStream
import java.io.InputStreamReader
import java.net.InetSocketAddress
import java.net.Socket
import java.util.Properties
import kotlin.concurrent.thread

const val ZOOKEEPER_FOURLEXCEPTION = "4LEXCEPTION"

enum class ZookeeperCMDRSP(val cmd: String, val rsp: String) {
    RUOK("ruok", "imok\n"),
    REQS("reqs", "") // prerequisite is idle zookeeper
}

class ZKServer(override val port: Int, private val dataDir: File, private val withSecurity: Boolean) : ServerBase() {

    // see link for ZooKeeperServerMain below for starting up embeddedzookeeper
    // https://github.com/apache/zookeeper/blob/branch-3.4.13/src/java/main/org/apache/zookeeper/server/ZooKeeperServerMain.java

    override val url = "$host:$port"

    // not possible to stop and restart zookeeper, use core inner class
    private class ZKS(port: Int, dataDir: File, withSecurity: Boolean) : ZooKeeperServerMain() {

        private val propsBasic = Properties().apply {
            set("dataDir", dataDir.toString())
            set("clientPort", "$port")
            set("maxClientCnxns", "0")
            if (withSecurity) {
                set("authProvider.1", "org.apache.zookeeper.server.auth.SASLAuthenticationProvider")
                set("requireClientAuthScheme", "sasl")
                set("jaasLoginRenew", "3600000")
            }
        }

        private val propFile = "$dataDir/embeddedzk.properties".also { fName ->
            try {
                File(fName).let { f ->
                    FileUtils.forceMkdir(dataDir)
                    FileOutputStream(f).use { out -> propsBasic.store(out, "") }
                }
            } catch (e: Exception) { /*  will get error when starting zookeeper */ }
        }

        private fun start() {
            initializeAndRun(arrayOf(propFile)) // start point, avoiding System.exit in main
            shutdown() // must shutdown connection factory in order to release port
        }

        // start zookeeper in a thread
        val zkThread = thread { start() }
    }

    private val zk = mutableListOf<ZKS>()

    private fun waitForZookeeperOk(): Boolean =
    // sequence and lazy evaluation, will finish at the first true element in any
            (1..40).asSequence()
                    .map {
                        Thread.sleep(20)
                        send4LCommand(ZookeeperCMDRSP.RUOK.cmd)
                    }.any { it == ZookeeperCMDRSP.RUOK.rsp }

    override fun start() = when (status) {
        ServerStatus.NotRunning -> {
            zk.add(ZKS(port, dataDir, withSecurity))
            waitForZookeeperOk()
            status = ServerStatus.Running
        }
        else -> {}
    }

    override fun stop() = when (status) {
        ServerStatus.Running -> {
            val zks = zk.first()

            // stop thread, see InterruptedException in zookeeper initializeAndRun
            try { zks.zkThread.interrupt() } catch (e: Exception) { }
            try { zks.zkThread.join() } catch (e: Exception) { }

            zk.removeAll { true }
            status = ServerStatus.NotRunning
        }
        else -> {}
    }

    // minimum code for send and receive 4L commands
    // will return the response or string fourLEXCEPTION

    fun send4LCommand(cmd: String, timeout: Int = 100): String =
            Socket()
                    .apply { this.soTimeout = timeout }
                    .use { socket ->
                        try { socket.connect(InetSocketAddress(host, port), timeout) } catch (e: Exception) { }
                        when (socket.isConnected) {
                            false -> ZOOKEEPER_FOURLEXCEPTION
                            else -> {
                                // a couple of functions using socket in scope
                                val sndCmd: () -> Boolean = {
                                    try {
                                        socket.getOutputStream().let { os ->
                                            os.write(cmd.toByteArray())
                                            os.flush()
                                            socket.shutdownOutput()
                                        }
                                        true
                                    } catch (e: Exception) { false }
                                }

                                val rcvRsp: () -> String = {
                                    try {
                                        BufferedReader(InputStreamReader(socket.getInputStream()))
                                                .use { br -> br.readLines() }
                                                .fold("") { res, str -> res + str + "\n" }
                                    } catch (e: Exception) { ZOOKEEPER_FOURLEXCEPTION }
                                }

                                if (sndCmd()) rcvRsp() else ZOOKEEPER_FOURLEXCEPTION
                            }
                        }
                    }
}