package no.nav.common.embeddedzookeeper

import no.nav.common.embeddedutils.ServerBase
import no.nav.common.embeddedutils.ServerStatus
import org.apache.zookeeper.server.ZooKeeperServerMain
import java.io.BufferedReader
import java.io.InputStreamReader
import java.net.InetSocketAddress
import java.net.Socket
import java.nio.file.Files
import java.nio.file.Path
import java.util.Properties
import kotlin.concurrent.thread

const val ZOOKEEPER_FOURLEXCEPTION = "4LEXCEPTION"

enum class ZookeeperCMDRSP(val cmd: String, val rsp: String) {
    RUOK("ruok", "imok\n"),
    REQS("reqs", ""), // prerequisite is idle zookeeper
}

class ZKServer(override val port: Int, private val dataDir: Path, private val withSecurity: Boolean) : ServerBase() {
    // see link for ZooKeeperServerMain below for starting up embeddedzookeeper
    // https://github.com/apache/zookeeper/blob/branch-3.4.13/src/java/main/org/apache/zookeeper/server/ZooKeeperServerMain.java

    override val url = "$host:$port"

    // not possible to stop and restart zookeeper, use core inner class
    private class ZKS(port: Int, dataDir: Path, withSecurity: Boolean) : ZooKeeperServerMain() {
        private val propsBasic =
            Properties().apply {
                set("dataDir", dataDir.toAbsolutePath().toString())
                set("clientPort", "$port")
                set("maxClientCnxns", "0")
                set("4lw.commands.whitelist", "reqs,ruok")
                set("admin.enableServer", "false")
                if (withSecurity) {
                    set("authProvider.1", "org.apache.zookeeper.server.auth.SASLAuthenticationProvider")
                    set("requireClientAuthScheme", "sasl")
                    set("jaasLoginRenew", "3600000")
                }
            }

        private val propFile =
            "$dataDir/embeddedzk.properties".also { fName ->
                try {
                    Files.createDirectories(dataDir)
                    Files.newOutputStream(dataDir.resolve(fName)).use { out ->
                        propsBasic.store(out, "")
                    }
                } catch (e: Exception) {
                    // will get error when starting zookeeper
                }
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
        (1..2000).asSequence()
            .map {
                Thread.sleep(50)
                send4LCommand(ZookeeperCMDRSP.RUOK.cmd)
            }.any { it == ZookeeperCMDRSP.RUOK.rsp }

    override fun start() {
        if (status == ServerStatus.NotRunning) {
            zk.add(ZKS(port, dataDir, withSecurity))
            waitForZookeeperOk()
            status = ServerStatus.Running
        }
    }

    override fun stop() {
        if (status == ServerStatus.Running) {
            val zks = zk.first()

            // stop thread, see InterruptedException in zookeeper initializeAndRun
            runCatching {
                zks.zkThread.interrupt()
            }.getOrNull()

            runCatching {
                zks.zkThread.join()
            }.getOrNull()

            zk.removeAll { true }
            status = ServerStatus.NotRunning
        }
    }

    // minimum code for send and receive 4L commands
    // will return the response or string fourLEXCEPTION

    fun send4LCommand(
        cmd: String,
        timeout: Int = 100,
    ): String =
        Socket()
            .apply { this.soTimeout = timeout }
            .use { socket ->
                runCatching {
                    socket.connect(InetSocketAddress(host, port), timeout)
                }.getOrNull()
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
                            } catch (e: Exception) {
                                false
                            }
                        }

                        val rcvRsp: () -> String = {
                            try {
                                BufferedReader(InputStreamReader(socket.getInputStream()))
                                    .use { br -> br.readLines() }
                                    .fold("") { res, str -> res + str + "\n" }
                            } catch (e: Exception) {
                                ZOOKEEPER_FOURLEXCEPTION
                            }
                        }

                        if (sndCmd()) rcvRsp() else ZOOKEEPER_FOURLEXCEPTION
                    }
                }
            }
}
