package no.nav.common.embeddedzookeeper

import no.nav.common.embeddedutils.appDirFor
import no.nav.common.embeddedutils.dataDirFor
import no.nav.common.embeddedutils.deleteDir
import no.nav.common.embeddedutils.getAvailablePort
import no.nav.common.setUpJAASContext
import org.amshove.kluent.shouldBeEqualTo
import org.spekframework.spek2.Spek
import org.spekframework.spek2.style.specification.Suite
import org.spekframework.spek2.style.specification.describe

object ZKServerSpec : Spek({
    val tests = ZookeeperCMDRSP.values().map { it.cmd to it.rsp }.toMap()

    val zookeeperBasePath = appDirFor("zookeeper")

    afterGroup { deleteDir(zookeeperBasePath) }

    fun testZKCmds(
        suite: Suite,
        zk: ZKServer,
        i: Int,
    ) = suite.apply {
        context("embedded zookeeper (start/commands/stop) - iteration: $i") {
            beforeGroup { zk.start() }
            tests.forEach { cmd, res ->
                it("should respond '$res' on command '$cmd'") { zk.send4LCommand(cmd) shouldBeEqualTo res }
            }
            afterGroup { zk.stop() }
        }
    }

    describe("zookeeper server tests without withSecurity") {

        val minSecurity = false
        val zk = ZKServer(getAvailablePort(), dataDirFor(zookeeperBasePath), minSecurity)

        (1..2).forEach { testZKCmds(this, zk, it) }
    }

    describe("zookeeper server tests with withSecurity") {

        val minSecurity = true
        setUpJAASContext()

        val zk = ZKServer(getAvailablePort(), dataDirFor(zookeeperBasePath), minSecurity)

        (1..2).forEach { testZKCmds(this, zk, it) }
    }
})
