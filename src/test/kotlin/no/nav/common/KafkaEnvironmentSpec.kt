package no.nav.common

import io.ktor.client.HttpClient
import io.ktor.client.engine.apache.Apache
import no.nav.common.embeddedzookeeper.ZookeeperCMDRSP
import no.nav.common.test.common.createConsumerACL
import no.nav.common.test.common.createProducerACL
import no.nav.common.test.common.httpReqResp
import no.nav.common.test.common.kafkaConsume
import no.nav.common.test.common.kafkaProduce
import no.nav.common.test.common.noOfBrokers
import no.nav.common.test.common.noOfTopics
import no.nav.common.test.common.scRegTests
import no.nav.common.test.common.topics
import org.amshove.kluent.shouldBeEqualTo
import org.amshove.kluent.shouldContainAll
import org.amshove.kluent.shouldEqualTo
import org.apache.kafka.clients.admin.AdminClient
import org.spekframework.spek2.Spek
import org.spekframework.spek2.style.specification.describe

object KafkaEnvironmentSpec : Spek({

    lateinit var adminClient: AdminClient // used across describes, closed after use

    val basicTests: (Int, List<String>) -> List<Triple<String, (AdminClient) -> Int, Int>> = { nB, t ->
        listOf(
            Triple("should have '$nB' of broker(s)", noOfBrokers, nB),
            Triple("should have '${t.size}' topics available", noOfTopics, t.size)
        )
    }

    describe("default kafka environment") {

        val keDefault = KafkaEnvironment()

        beforeGroup {
            keDefault.start()
            adminClient = keDefault.adminClient!!
        }

        context("basic verification") {

            it("should have 1 zookeeper with status ok") {
                keDefault.zookeeper.send4LCommand(ZookeeperCMDRSP.RUOK.cmd) shouldBeEqualTo ZookeeperCMDRSP.RUOK.rsp
            }

            basicTests(1, emptyList()).forEach { it(it.first) { it.second(adminClient) shouldEqualTo it.third } }
        }

        afterGroup {
            adminClient.close()
            keDefault.tearDown()
        }
    }

    describe("basic kafka environment") {

        val basicTopics = listOf("basic01", "basic02")
        val keBasic = KafkaEnvironment(topics = basicTopics)

        beforeGroup {
            keBasic.start()
            adminClient = keBasic.adminClient!!
        }

        context("basic verification") {

            it("should have 1 zookeeper with status ok") {
                keBasic.zookeeper.send4LCommand(ZookeeperCMDRSP.RUOK.cmd) shouldBeEqualTo ZookeeperCMDRSP.RUOK.rsp
            }

            basicTests(1, basicTopics).forEach { it(it.first) { it.second(adminClient) shouldEqualTo it.third } }
        }

        it("should have topics $basicTopics available") {
            topics(adminClient) shouldContainAll basicTopics
        }

        afterGroup {
            adminClient.close()
            keBasic.tearDown()
        }
    }

    describe("kafka environment with 0 broker(s)") {

        val kEnv0 = KafkaEnvironment(noOfBrokers = 0)

        beforeGroup {
            kEnv0.start()
        }

        it("should have 1 zookeeper with status ok") {
            kEnv0.zookeeper.send4LCommand(ZookeeperCMDRSP.RUOK.cmd) shouldBeEqualTo ZookeeperCMDRSP.RUOK.rsp
        }

        it("should have 0 broker") {
            kEnv0.brokers.size shouldEqualTo 0
        }

        afterGroup {
            kEnv0.tearDown()
        }
    }

    describe("kafka environment with 0 brokers and schema registry") {

        val schema = listOf("_schemas")
        val kEnv1 = KafkaEnvironment(noOfBrokers = 0, withSchemaRegistry = true)
        val client = HttpClient(Apache)

        beforeGroup {
            kEnv1.start()
            adminClient = kEnv1.adminClient!!
        }

        context("basic verification") {

            it("should have 1 zookeeper with status ok") {
                kEnv1.zookeeper.send4LCommand(ZookeeperCMDRSP.RUOK.cmd) shouldBeEqualTo ZookeeperCMDRSP.RUOK.rsp
            }

            basicTests(1, schema).forEach { it(it.first) { it.second(adminClient) shouldEqualTo it.third } }
        }

        it("should have topic(s) $schema available") {

            topics(adminClient) shouldContainAll schema
        }

        context("schema reg") {

            scRegTests.forEach { txt, cmdRes ->
                it(txt) { httpReqResp(client, kEnv1.schemaRegistry!!, cmdRes.first) shouldBeEqualTo cmdRes.second }
            }
        }

        afterGroup {
            adminClient.close()
            kEnv1.tearDown()
        }
    }

    describe("kafka environment with 0 brokers and 2 topics") {

        val basicTopics = listOf("basic01", "basic02")
        val kEnv2 = KafkaEnvironment(noOfBrokers = 0, topics = basicTopics)

        beforeGroup {
            kEnv2.start()
            adminClient = kEnv2.adminClient!!
        }

        context("basic verification") {

            it("should have 1 zookeeper with status ok") {
                kEnv2.zookeeper.send4LCommand(ZookeeperCMDRSP.RUOK.cmd) shouldBeEqualTo ZookeeperCMDRSP.RUOK.rsp
            }

            basicTests(1, basicTopics).forEach { it(it.first) { it.second(adminClient) shouldEqualTo it.third } }
        }

        it("should have topic(s) $basicTopics available") {
            topics(adminClient) shouldContainAll basicTopics
        }

        afterGroup {
            adminClient.close()
            kEnv2.tearDown()
        }
    }

    describe("kafka environment with min Security, 1 broker and one topic") {

        val topic = "basic01"
        val kEnv3 =  KafkaEnvironment(noOfBrokers = 1, topics = listOf(topic), minSecurity = true)

        val events = (1..9).map { "event$it" }

        beforeGroup {
            kEnv3.start()
            adminClient = kEnv3.adminClient!!
        }

        context("basic verification") {

            it("should have 1 zookeeper with status ok") {
                kEnv3.zookeeper.send4LCommand(ZookeeperCMDRSP.RUOK.cmd) shouldBeEqualTo ZookeeperCMDRSP.RUOK.rsp
            }

            basicTests(1, listOf(topic)).forEach { it(it.first) { it.second(adminClient) shouldEqualTo it.third } }
        }

        it("should have topic(s) '$topic' available") {
            topics(adminClient) shouldContainAll listOf(topic)
        }

        context("generate required producer and consumer ACLs for topic") {

            it("should successfully create producer ACL") {
                try {
                    adminClient.createAcls(createProducerACL(topic, kafkaP1.username)).all().get()
                    true
                } catch (e: Exception) { false } shouldEqualTo true
            }

            it("should successfully create consumer ACL") {
                try {
                    adminClient.createAcls(createConsumerACL(topic, kafkaC1.username)).all().get()
                    true
                } catch (e: Exception) { false } shouldEqualTo true
            }

        }

        it("should send all events $events to topic '$topic'") {
            kafkaProduce(
                    (kEnv3.serverPark.brokerStatus as KafkaEnvironment.BrokerStatus.Available).brokersURL,
                    topic,
                    kafkaP1.username,
                    kafkaP1.password,
                    events) shouldEqualTo true
        }

        it("should consume all events $events from topic '$topic'") {
            val fetchedEvents = kafkaConsume(
                    (kEnv3.serverPark.brokerStatus as KafkaEnvironment.BrokerStatus.Available).brokersURL,
                    topic,
                    kafkaC1.username,
                    kafkaC1.password,
                    events.last())

            fetchedEvents shouldContainAll events
        }

        afterGroup {
            adminClient.close()
            kEnv3.tearDown()
        }

    }
})