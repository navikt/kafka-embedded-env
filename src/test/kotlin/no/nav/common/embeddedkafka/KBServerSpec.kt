package no.nav.common.embeddedkafka

import no.nav.common.KafkaEnvironment
import no.nav.common.test.common.TxtCmdRes
import no.nav.common.test.common.noOfBrokers
import no.nav.common.test.common.noOfTopics
import org.amshove.kluent.shouldEqualTo
import org.apache.kafka.clients.admin.AdminClient
import org.spekframework.spek2.Spek
import org.spekframework.spek2.style.specification.describe

object KBServerSpec : Spek({

    // too much housekeeping involved starting brokers without KafkaEnvironment

    val tests2B = listOf(
        TxtCmdRes("should report 2 brokers", noOfBrokers, 2),
        TxtCmdRes("should report 0 topics", noOfTopics, 0)
    )
    val tests1B = listOf(
        TxtCmdRes("should report 1 broker", noOfBrokers, 1),
        TxtCmdRes("should report 0 topics", noOfTopics, 0)
    )

    describe("kafka broker tests without withSecurity") {

        val env = KafkaEnvironment(noOfBrokers = 2)
        var ac: AdminClient? = null

        beforeGroup {
            env.start()
            ac = env.adminClient
        }

        context("active embedded kafka cluster of two brokers") {
            tests2B.forEach { it(it.txt) { it.cmd(ac) shouldEqualTo it.res } }
        }

        context("active embedded kafka cluster with 1 stopped broker") {
            beforeGroup { env.brokers.last().stop() }
            tests1B.forEach { it(it.txt) { it.cmd(ac) shouldEqualTo it.res } }
        }

        context("active embedded kafka cluster of 1 restarted broker") {
            beforeGroup { env.brokers.last().start() }
            tests2B.forEach { it(it.txt) { it.cmd(ac) shouldEqualTo it.res } }
        }

        afterGroup {
            ac?.close()
            env.tearDown()
        }
    }

    describe("kafka broker tests with withSecurity") {

        val env = KafkaEnvironment(noOfBrokers = 1, withSecurity = true)
        var ac: AdminClient? = null

        beforeGroup {
            env.start()
            ac = env.adminClient
        }

        tests1B.forEach { it(it.txt) { it.cmd(ac) shouldEqualTo it.res } }

        afterGroup {
            ac?.close()
            env.tearDown()
        }
    }
})