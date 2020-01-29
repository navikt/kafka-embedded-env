package no.nav.common

import io.ktor.client.HttpClient
import io.ktor.client.engine.apache.Apache
import kotlinx.coroutines.runBlocking
import no.nav.common.embeddedzookeeper.ZookeeperCMDRSP
import no.nav.common.test.common.TxtCmdRes
import no.nav.common.test.common.createConsumerACL
import no.nav.common.test.common.createProducerACL
import no.nav.common.test.common.httpReqResp
import no.nav.common.test.common.kafkaAvroConsume
import no.nav.common.test.common.kafkaAvroProduce
import no.nav.common.test.common.kafkaConsume
import no.nav.common.test.common.kafkaProduce
import no.nav.common.test.common.noOfBrokers
import no.nav.common.test.common.noOfTopics
import no.nav.common.test.common.scRegTests
import no.nav.common.test.common.topics
import org.amshove.kluent.`should not be`
import org.amshove.kluent.shouldBeEqualTo
import org.amshove.kluent.shouldBeGreaterOrEqualTo
import org.amshove.kluent.shouldContainAll
import org.amshove.kluent.shouldEqual
import org.amshove.kluent.shouldEqualTo
import org.amshove.kluent.shouldNotBe
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
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

        context("state tests - Initialized") {

            it("should have state initialized for ServerPark") {
                env.serverPark.status shouldEqual KafkaEnvironment.ServerParkStatus.Initialized
            }

            it("should have state Available for brokerStatus") {
                env.serverPark.brokerStatus shouldNotBe KafkaEnvironment.BrokerStatus.NotAvailable
                env.brokers.size shouldEqualTo 1
                env.brokersURL.length shouldBeGreaterOrEqualTo 1
            }

            it("should return null for adminClient") {
                env.adminClient shouldEqual null
            }

            it("should have state NotAvailable for schemaRegStatus") {
                env.serverPark.schemaRegStatus shouldEqual KafkaEnvironment.SchemaRegistryStatus.NotAvailable
            }
        }

        context("basic verification") {

            beforeGroup {
                env.start()
                ac = env.adminClient
            }

            it("should have state Started for ServerPark") {
                env.serverPark.status shouldEqual KafkaEnvironment.ServerParkStatus.Started
            }

            it("should have 1 zookeeper with status ok") {
                env.zookeeper.send4LCommand(ZookeeperCMDRSP.RUOK.cmd) shouldBeEqualTo ZookeeperCMDRSP.RUOK.rsp
            }

            getTests(1, topics).forEach { it(it.txt) { it.cmd(ac) shouldEqualTo it.res } }

            afterGroup {
                ac?.close()
                env.tearDown()
            }
        }

        context("state tests - epilogue") {

            it("should have state TearDownCompleted for ServerPark") {
                env.serverPark.status shouldEqual KafkaEnvironment.ServerParkStatus.TearDownCompleted
            }

            it("should have state NotAvailable for brokerStatus") {
                env.serverPark.brokerStatus shouldEqual KafkaEnvironment.BrokerStatus.NotAvailable
            }

            it("should return null for adminClient") {
                env.adminClient shouldEqual null
            }

            it("should have state NotAvailable for schemaRegStatus") {
                env.serverPark.schemaRegStatus shouldEqual KafkaEnvironment.SchemaRegistryStatus.NotAvailable
            }
        }
    }

    describe("basic kafka environment") {

        val topics = listOf("basic01", "basic02")
        val env = KafkaEnvironment(topicNames = topics)
        var ac: AdminClient? = null

        beforeGroup {
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

        afterGroup {
            ac?.close()
            env.tearDown()
        }
    }

    describe("basic kafka environment with topicInfos and topicNames") {

        val topicNames = listOf("basic01", "basic02")
        val topicInfos = listOf(
                KafkaEnvironment.TopicInfo("advanced01", 4, mapOf("retention.ms" to "5000"))
        )

        val expectedTopicNames = topicNames + topicInfos.map { it.name }

        val env = KafkaEnvironment(topicNames = topicNames, topicInfos = topicInfos)
        var ac: AdminClient? = null

        beforeGroup {
            env.start()
            ac = env.adminClient
        }

        context("basic verification") {

            it("should have 1 zookeeper with status ok") {
                env.zookeeper.send4LCommand(ZookeeperCMDRSP.RUOK.cmd) shouldBeEqualTo ZookeeperCMDRSP.RUOK.rsp
            }

            getTests(1, expectedTopicNames).forEach { it(it.txt) { it.cmd(ac) shouldEqualTo it.res } }
        }

        it("should have topic(s) $expectedTopicNames available") {
            ac.topics() shouldContainAll expectedTopicNames
        }

        afterGroup {
            ac?.close()
            env.tearDown()
        }
    }

    describe("kafka environment with 3 broker(s)") {

        val env = KafkaEnvironment(noOfBrokers = 3)

        beforeGroup {
            env.start()
        }

        it("should have 1 zookeeper with status ok") {
            env.zookeeper.send4LCommand(ZookeeperCMDRSP.RUOK.cmd) shouldBeEqualTo ZookeeperCMDRSP.RUOK.rsp
        }

        it("should have maximum of 2 broker(s)") {
            env.brokers.size shouldEqualTo 2
        }

        afterGroup {
            env.tearDown()
        }
    }

    describe("kafka environment with 0 broker(s)") {

        val env = KafkaEnvironment(noOfBrokers = 0)

        beforeGroup {
            env.start()
        }

        it("should have 1 zookeeper with status ok") {
            env.zookeeper.send4LCommand(ZookeeperCMDRSP.RUOK.cmd) shouldBeEqualTo ZookeeperCMDRSP.RUOK.rsp
        }

        it("should have 0 broker") {
            env.brokers.size shouldEqualTo 0
        }

        afterGroup {
            env.tearDown()
        }
    }

    describe("kafka environment with 0 brokers and schema registry") {

        val topics = listOf("_schemas") // automatically created by schema registry
        val env = KafkaEnvironment(noOfBrokers = 0, withSchemaRegistry = true)
        var ac: AdminClient? = null
        val client = HttpClient(Apache)

        beforeGroup {
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

        afterGroup {
            ac?.close()
            env.tearDown()
        }
    }

    describe("kafka environment with 0 brokers and 2 topics") {

        val topics = listOf("basic01", "basic02")
        val env = KafkaEnvironment(noOfBrokers = 0, topicNames = topics)
        var ac: AdminClient? = null

        beforeGroup {
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

        afterGroup {
            ac?.close()
            env.tearDown()
        }
    }

    describe("kafka environment with security, custom JAASCredential") {

        val prod = JAASCredential("myP1", "myP1p")
        val cons = JAASCredential("myC1", "myC1p")
        val users = listOf(prod, cons)
        val topics = listOf("custom01")
        val env = KafkaEnvironment(noOfBrokers = 1, topicNames = topics, withSecurity = true, users = users)
        var ac: AdminClient? = null

        val events = (1..9).map { "$it" to "event$it" }.toMap()

        beforeGroup {
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

        afterGroup {
            ac?.close()
            env.tearDown()
        }
    }

    describe("kafka environment with security, invalid JAASCredential") {

        val prod = JAASCredential("invalidP1", "invP1p")
        val cons = JAASCredential("invalidC1", "invC1p")
        val topics = listOf("invalid01")
        val env = KafkaEnvironment(noOfBrokers = 1, topicNames = topics, withSecurity = true) // not adding users
        var ac: AdminClient? = null

        val events = (1..9).map { "$it" to "event$it" }.toMap()

        beforeGroup {
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

        afterGroup {
            ac?.close()
            env.tearDown()
        }
    }

    describe("kafka environment with security, predefined JAASCredential") {

        val topics = listOf("basic01")
        val env = KafkaEnvironment(noOfBrokers = 1, topicNames = topics, withSecurity = true)
        var ac: AdminClient? = null

        val events = (1..9).map { "$it" to "event$it" }.toMap()

        beforeGroup {
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

        afterGroup {
            ac?.close()
            env.tearDown()
        }
    }

    describe("kafka environment with security and schema registry") {
        val topics = listOf("basicAvroRecord01")
        val env = KafkaEnvironment(topicNames = topics, withSecurity = true, withSchemaRegistry = true)
        var ac: AdminClient? = null

        val schemaSource = """
                  {
                      "namespace" : "no.nav.common",
                      "type" : "record",
                      "name" : "BasicAvroRecord",
                      "fields" : [
                        {"name": "number", "type": "int"}
                      ]
                    }

                """.trimIndent()

        val avroSchema = Schema.Parser().parse(schemaSource)

        val events: Map<String, GenericRecord> = (1..9).map {
            "$it" to
                    GenericData.Record(avroSchema).apply {
                        put("number", it)
                    }
        }.toMap()

        beforeGroup {
            env.start()
            ac = env.adminClient
        }

        it("should have topic(s) '$topics' available") {
            ac.topics() shouldContainAll topics
        }

        it("should have a  schema registry running ") {
            env.schemaRegistry `should not be` null
        }

        it("should send all avro events $events to topic '$topics'") {
            runBlocking {
                kafkaAvroProduce(
                        env.brokersURL,
                        env.schemaRegistry!!.url,
                        topics.first(),
                        kafkaClient.username,
                        kafkaClient.password,
                        events)
            } shouldEqualTo true
        }

        it("should consume all avro events $events from topic '$topics'") {
            runBlocking {
                kafkaAvroConsume(
                        env.brokersURL,
                        env.schemaRegistry!!.url,
                        topics.first(),
                        kafkaClient.username,
                        kafkaClient.password,
                        events.size)
            } shouldContainAll events
        }

        afterGroup {
            ac?.close()
            env.tearDown()
        }
    }
})
