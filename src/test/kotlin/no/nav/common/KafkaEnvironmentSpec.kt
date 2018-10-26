package no.nav.common

import io.ktor.client.HttpClient
import io.ktor.client.engine.apache.Apache
import kotlinx.coroutines.experimental.runBlocking
import no.nav.common.embeddedzookeeper.ZookeeperCMDRSP
import no.nav.common.test.common.TxtCmdRes
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

    fun getTests(noBrokers: Int, topics: List<String>): List<TxtCmdRes> = listOf(
            TxtCmdRes("should have '$noBrokers' of broker(s)", noOfBrokers, noBrokers),
            TxtCmdRes("should have '${topics.size}' topics available", noOfTopics, topics.size)
    )

    describe("default kafka environment") {

        val topics = emptyList<String>()
        val env = KafkaEnvironment()
        var ac: AdminClient? = null

        before {
            env.start()
            ac = env.adminClient
        }

        context("basic verification") {

            it("should have 1 zookeeper with status ok") {
                env.zookeeper.send4LCommand(ZookeeperCMDRSP.RUOK.cmd) shouldBeEqualTo ZookeeperCMDRSP.RUOK.rsp
            }

            getTests(1, topics).forEach { it(it.txt) { it.cmd(ac) shouldEqualTo it.res } }
        }

        after {
            ac?.close()
            env.tearDown()
        }
    }

    describe("basic kafka environment") {

        val topics = listOf("basic01", "basic02")
        val env = KafkaEnvironment(topics = topics)
        var ac: AdminClient? = null

        before {
            env.start()
            ac = env.adminClient
        }

        context("basic verification") {

            it("should have 1 zookeeper with status ok") {
                env.zookeeper.send4LCommand(ZookeeperCMDRSP.RUOK.cmd) shouldBeEqualTo ZookeeperCMDRSP.RUOK.rsp
            }

            getTests(1, topics).forEach { it(it.txt) { it.cmd(ac) shouldEqualTo it.res } }
        }

        it("should have topic(s) $topics available") {
            ac.topics() shouldContainAll topics
        }

        after {
            ac?.close()
            env.tearDown()
        }
    }

    describe("kafka environment with 3 broker(s)") {

        val env = KafkaEnvironment(noOfBrokers = 3)

        before {
            env.start()
        }

        it("should have 1 zookeeper with status ok") {
            env.zookeeper.send4LCommand(ZookeeperCMDRSP.RUOK.cmd) shouldBeEqualTo ZookeeperCMDRSP.RUOK.rsp
        }

        it("should have maximum of 2 broker(s)") {
            env.brokers.size shouldEqualTo 2
        }

        after {
            env.tearDown()
        }
    }

    describe("kafka environment with 0 broker(s)") {

        val env = KafkaEnvironment(noOfBrokers = 0)

        before {
            env.start()
        }

        it("should have 1 zookeeper with status ok") {
            env.zookeeper.send4LCommand(ZookeeperCMDRSP.RUOK.cmd) shouldBeEqualTo ZookeeperCMDRSP.RUOK.rsp
        }

        it("should have 0 broker") {
            env.brokers.size shouldEqualTo 0
        }

        after {
            env.tearDown()
        }
    }

    describe("kafka environment with 0 brokers and schema registry") {

        val topics = listOf("_schemas") // automatically created by schema registry
        val env = KafkaEnvironment(noOfBrokers = 0, withSchemaRegistry = true)
        var ac: AdminClient? = null
        val client = HttpClient(Apache)

        before {
            env.start()
            ac = env.adminClient
        }

        context("basic verification") {

            it("should have 1 zookeeper with status ok") {
                env.zookeeper.send4LCommand(ZookeeperCMDRSP.RUOK.cmd) shouldBeEqualTo ZookeeperCMDRSP.RUOK.rsp
            }

            getTests(1, topics).forEach { it(it.txt) { it.cmd(ac) shouldEqualTo it.res } }
        }

        it("should have topic(s) $topics available") {

            ac.topics() shouldContainAll topics
        }

        context("schema reg") {

            scRegTests.forEach { txt, cmdRes ->
                it(txt) { httpReqResp(client, env.schemaRegistry!!, cmdRes.first) shouldBeEqualTo cmdRes.second }
            }
        }

        after {
            ac?.close()
            env.tearDown()
        }
    }

    describe("kafka environment with 0 brokers and 2 topics") {

        val topics = listOf("basic01", "basic02")
        val env = KafkaEnvironment(noOfBrokers = 0, topics = topics)
        var ac: AdminClient? = null

        before {
            env.start()
            ac = env.adminClient
        }

        context("basic verification") {

            it("should have 1 zookeeper with status ok") {
                env.zookeeper.send4LCommand(ZookeeperCMDRSP.RUOK.cmd) shouldBeEqualTo ZookeeperCMDRSP.RUOK.rsp
            }

            getTests(1, topics).forEach { it(it.txt) { it.cmd(ac) shouldEqualTo it.res } }
        }

        it("should have topic(s) $topics available") {
            ac.topics() shouldContainAll topics
        }

        after {
            ac?.close()
            env.tearDown()
        }
    }

    describe("kafka environment with security, custom JAASCredential") {

        val prod = JAASCredential("myP1", "myP1p")
        val cons = JAASCredential("myC1", "myC1p")
        val users = listOf(prod, cons)
        val topics = listOf("custom01")
        val env = KafkaEnvironment(noOfBrokers = 1, topics = topics, withSecurity = true, users = users)
        var ac: AdminClient? = null

        val events = (1..9).map { "$it" to "event$it" }.toMap()

        before {
            env.start()
            ac = env.adminClient
        }

        context("basic verification") {

            it("should have 1 zookeeper with status ok") {
                env.zookeeper.send4LCommand(ZookeeperCMDRSP.RUOK.cmd) shouldBeEqualTo ZookeeperCMDRSP.RUOK.rsp
            }

            getTests(1, topics).forEach { it(it.txt) { it.cmd(ac) shouldEqualTo it.res } }
        }

        it("should have topic(s) '$topics' available") {
            ac.topics() shouldContainAll topics
        }

        context("generate required producer and consumer ACLs for topic") {

            it("should successfully create producer ACL") {
                ac?.let {
                    try {
                        it.createAcls(createProducerACL(mapOf(topics.first() to prod.username))).all().get()
                        true
                    } catch (e: Exception) { false }
                } ?: false shouldEqualTo true
            }

            it("should successfully create consumer ACL") {
                ac?.let {
                    try {
                        it.createAcls(createConsumerACL(mapOf(topics.first() to cons.username))).all().get()
                        true
                    } catch (e: Exception) { false } } ?: false shouldEqualTo true
            }
        }

        it("should send all events $events to topic '$topics'") {
            runBlocking {
                kafkaProduce(
                        env.brokersURL,
                        topics.first(),
                        prod.username,
                        prod.password,
                        events)
            } shouldEqualTo true
        }

        it("should consume all events $events from topic '$topics'") {
            runBlocking {
                kafkaConsume(
                        env.brokersURL,
                        topics.first(),
                        cons.username,
                        cons.password,
                        events.size)
            } shouldContainAll events
        }

        after {
            ac?.close()
            env.tearDown()
        }
    }

    describe("kafka environment with security, invalid JAASCredential") {

        val prod = JAASCredential("invalidP1", "invP1p")
        val cons = JAASCredential("invalidC1", "invC1p")
        val topics = listOf("invalid01")
        val env = KafkaEnvironment(noOfBrokers = 1, topics = topics, withSecurity = true) // not adding users
        var ac: AdminClient? = null

        val events = (1..9).map { "$it" to "event$it" }.toMap()

        before {
            env.start()
            ac = env.adminClient
        }

        it("should have topic(s) '$topics' available") {
            ac.topics() shouldContainAll topics
        }

        context("generate required producer and consumer ACLs for topic") {

            it("should successfully create producer ACL") {
                ac?.let {
                    try {
                        it.createAcls(createProducerACL(mapOf(topics.first() to prod.username))).all().get()
                        true
                    } catch (e: Exception) { false }
                } ?: false shouldEqualTo true
            }

            it("should successfully create consumer ACL") {
                ac?.let {
                    try {
                        it.createAcls(createConsumerACL(mapOf(topics.first() to cons.username))).all().get()
                        true
                    } catch (e: Exception) { false } } ?: false shouldEqualTo true
            }
        }

        it("should not send any events $events to topic '$topics'") {
            runBlocking {
                kafkaProduce(
                        env.brokersURL,
                        topics.first(),
                        prod.username,
                        prod.password,
                        events)
            } shouldEqualTo false
        }

        it("should not consume any events $events from topic '$topics'") {
            runBlocking {
                kafkaConsume(
                        env.brokersURL,
                        topics.first(),
                        cons.username,
                        cons.password,
                        events.size)
            } shouldContainAll emptyMap()
        }

        after {
            ac?.close()
            env.tearDown()
        }
    }

    describe("kafka environment with security, predefined JAASCredential") {

        val topics = listOf("basic01")
        val env = KafkaEnvironment(noOfBrokers = 1, topics = topics, withSecurity = true)
        var ac: AdminClient? = null

        val events = (1..9).map { "$it" to "event$it" }.toMap()

        before {
            env.start()
            ac = env.adminClient
        }

        it("should have topic(s) '$topics' available") {
            ac.topics() shouldContainAll topics
        }

        context("generate required producer and consumer ACLs for topic") {

            it("should successfully create producer ACL") {
                ac?.let {
                    try {
                        it.createAcls(createProducerACL(mapOf(topics.first() to kafkaP1.username))).all().get()
                        true
                    } catch (e: Exception) { false }
                } ?: false shouldEqualTo true
            }

            it("should successfully create consumer ACL") {
                ac?.let {
                    try {
                        it.createAcls(createConsumerACL(mapOf(topics.first() to kafkaC1.username))).all().get()
                        true
                    } catch (e: Exception) { false } } ?: false shouldEqualTo true
            }
        }

        it("should send all events $events to topic '$topics'") {
            runBlocking {
                kafkaProduce(
                        env.brokersURL,
                        topics.first(),
                        kafkaP1.username,
                        kafkaP1.password,
                        events)
            } shouldEqualTo true
        }

        it("should consume all events $events from topic '$topics'") {
            runBlocking {
                kafkaConsume(
                        env.brokersURL,
                        topics.first(),
                        kafkaC1.username,
                        kafkaC1.password,
                        events.size)
            } shouldContainAll events
        }

        after {
            ac?.close()
            env.tearDown()
        }
    }
})