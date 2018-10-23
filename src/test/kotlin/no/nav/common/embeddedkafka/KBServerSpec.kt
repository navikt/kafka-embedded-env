package no.nav.common.embeddedkafka

import no.nav.common.KafkaEnvironment
import no.nav.common.test.common.noOfBrokers
import no.nav.common.test.common.noOfTopics
import org.amshove.kluent.shouldEqualTo
import org.apache.kafka.clients.admin.AdminClient
import org.spekframework.spek2.Spek
import org.spekframework.spek2.style.specification.describe

object KBServerSpec : Spek({

    // too much housekeeping involved starting brokers without KafkaEnvironment

    var adminClient: AdminClient? = null

    val tests2B = listOf(
            Triple("should report 2 brokers", noOfBrokers, 2),
            Triple("should report 0 topics", noOfTopics, 0)
    )
    val tests1B = listOf(
            Triple("should report 1 broker", noOfBrokers, 1),
            Triple("should report 0 topics", noOfTopics, 0)
    )

    describe("kafka broker tests without minSecurity") {

        val kEnvKBSS = KafkaEnvironment(2)

        beforeGroup {
            kEnvKBSS.start()
            adminClient = kEnvKBSS.adminClient
        }

        context("active embeddedkafka cluster of two brokers") {
            tests2B.forEach { it(it.first) { it.second(adminClient!!) shouldEqualTo it.third } }
        }

        context("active embeddedkafka cluster with 1 stopped broker") {

            beforeGroup { kEnvKBSS.brokers.last().stop() }
            tests1B.forEach { it(it.first) { it.second(adminClient!!) shouldEqualTo it.third } }
        }

        context("active embeddedkafka cluster of 1 restarted broker") {

            beforeGroup { kEnvKBSS.brokers.last().start() }
            tests2B.forEach { it(it.first) { it.second(adminClient!!) shouldEqualTo it.third } }
        }

        afterGroup {
            adminClient!!.close()
            kEnvKBSS.tearDown()
        }
    }

    describe("kafka broker tests with minSecurity") {

        val kEnvKBSSSec = KafkaEnvironment(noOfBrokers = 1, minSecurity = true)

        beforeGroup {
            kEnvKBSSSec.start()
            adminClient = kEnvKBSSSec.adminClient
        }

        tests1B.forEach { it(it.first) { it.second(adminClient!!) shouldEqualTo it.third } }

        afterGroup {
            adminClient!!.close()
            kEnvKBSSSec.tearDown()
        }
    }
})