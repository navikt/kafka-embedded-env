package no.nav.common.embeddedzookeeper

import no.nav.common.JAASContext
import no.nav.common.embeddedutils.getAvailablePort
import org.amshove.kluent.shouldBeEqualTo
import org.apache.commons.io.FileUtils
import org.spekframework.spek2.Spek
import org.spekframework.spek2.style.specification.Suite
import org.spekframework.spek2.style.specification.describe
import java.io.File
import java.io.IOException

object ZKServerSpec : Spek({

    val zkDataDir = File(System.getProperty("java.io.tmpdir"), "inmzookeeper").apply {
        // in case of fatal failure and no deletion in previous run
        try { FileUtils.deleteDirectory(this) } catch (e: IOException) { /* tried at least */ }
    }

    val tests = ZookeeperCMDRSP.values().map { it.cmd to it.rsp }.toMap()

    fun testZKCmds(suite: Suite, zk: ZKServer, i: Int) = suite.apply {
        context("embedded zookeeper (start/commands/stop) - iteration: $i") {
            beforeGroup { zk.start() }
            tests.forEach { cmd, res ->
                it("should respond '$res' on command '$cmd'") { zk.send4LCommand(cmd) shouldBeEqualTo res }
            }
            afterGroup { zk.stop() }
        }
    }

    describe("zookeeper server tests without minSecurity") {

        val minSecurity = false
        val zk = ZKServer(getAvailablePort(), zkDataDir, minSecurity)

        (1..2).forEach { testZKCmds(this, zk, it) }
    }

    describe("zookeeper server tests with minSecurity") {

        val minSecurity = true
        JAASContext.setUp()

        val zk = ZKServer(getAvailablePort(), zkDataDir, minSecurity)

        (1..2).forEach { testZKCmds(this, zk, it) }

        afterGroup {
            try { FileUtils.deleteDirectory(zkDataDir) } catch (e: IOException) { /* tried at least */ }
        }
    }
})