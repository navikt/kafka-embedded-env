package no.nav.common

import io.ktor.client.HttpClient
import io.ktor.client.engine.apache.Apache
import kotlinx.coroutines.experimental.runBlocking
import no.nav.common.test.common.SCHEMAREG_DefaultCompatibilityLevel
import no.nav.common.test.common.SCHEMAREG_NoSubjects
import no.nav.common.test.common.getSomething
import org.amshove.kluent.shouldBeEqualTo
import org.amshove.kluent.shouldEqualTo
import org.amshove.kluent.shouldContainAll
import org.apache.zookeeper.client.FourLetterWordMain
import org.spekframework.spek2.Spek
import org.spekframework.spek2.style.specification.describe
import java.net.URL

object KafkaEnvironmentSpec : Spek({

    describe("kafka environment tests") {

        context("default kafka environment") {

            val nBroker = 1
            val nTopics = 0
            val keDefault = KafkaEnvironment()

            beforeGroup {
                keDefault.start()
            }

            it("should have 1 zookeeper") {

                FourLetterWordMain.send4LetterWord(
                        keDefault.serverPark.zookeeper.host,
                        keDefault.serverPark.zookeeper.port,
                        "ruok") shouldBeEqualTo "imok\n"
            }

            it("should have $nBroker broker") {

                keDefault.adminClient.describeCluster().nodes().get().toList().size shouldEqualTo nBroker
            }

            it("should have $nTopics topics available") {

                keDefault.adminClient.listTopics().names().get().toList().size shouldEqualTo nTopics
            }

            afterGroup {
                keDefault.tearDown()
            }
        }

        context("basic kafka environment") {

            val basicTopics = listOf("basic01", "basic02")
            val nBroker = 1
            val keBasic = KafkaEnvironment(noOfBrokers = nBroker, topics = basicTopics)

            beforeGroup {
                keBasic.start()
            }

            it("should have 1 zookeeper") {

                FourLetterWordMain.send4LetterWord(
                        keBasic.serverPark.zookeeper.host,
                        keBasic.serverPark.zookeeper.port,
                        "ruok") shouldBeEqualTo "imok\n"
            }

            it("should have $nBroker broker") {

                keBasic.adminClient.describeCluster().nodes().get().toList().size shouldEqualTo nBroker
            }

            it("should have ${basicTopics.size} topics available") {

                keBasic.adminClient.listTopics().names().get().toList().size shouldEqualTo basicTopics.size
            }

            it("should have topics as requested available") {

                keBasic.adminClient.listTopics().names().get().toList() shouldContainAll basicTopics
            }

            afterGroup {
                keBasic.tearDown()
            }
        }

        context("kafka environment with 0 brokers") {

            val kEnv = KafkaEnvironment(0)
            val nBroker = 0

            beforeGroup {
                kEnv.start()
            }

            it("should have 1 zookeeper") {

                FourLetterWordMain.send4LetterWord(
                        kEnv.serverPark.zookeeper.host,
                        kEnv.serverPark.zookeeper.port,
                        "ruok") shouldBeEqualTo "imok\n"
            }

            it("should have $nBroker broker") {

                // Cannot initialize AdminClient, verify empty brokersURL instead
                kEnv.serverPark.brokers.size shouldEqualTo 0
            }

            afterGroup {
                kEnv.tearDown()
            }
        }

        context("kafka environment with 0 brokers and 2 topics") {

            val basicTopics = listOf("basic01", "basic02")
            val kEnv = KafkaEnvironment(noOfBrokers = 0, topics = basicTopics)
            val nBroker = 1

            beforeGroup {
                kEnv.start()
            }

            it("should have 1 zookeeper") {

                FourLetterWordMain.send4LetterWord(
                        kEnv.serverPark.zookeeper.host,
                        kEnv.serverPark.zookeeper.port,
                        "ruok") shouldBeEqualTo "imok\n"
            }

            it("should have $nBroker broker") {

                kEnv.adminClient.describeCluster().nodes().get().toList().size shouldEqualTo nBroker
            }

            it("should have ${basicTopics.size} topics available") {

                kEnv.adminClient.listTopics().names().get().toList().size shouldEqualTo basicTopics.size
            }

            it("should have topics as requested available") {

                kEnv.adminClient.listTopics().names().get().toList() shouldContainAll basicTopics
            }

            afterGroup {
                kEnv.tearDown()
            }
        }

        context("kafka environment with 0 brokers, 2 topics and schema registry") {

            val basicTopics = listOf("basic01", "basic02")
            val kEnv = KafkaEnvironment(noOfBrokers = 0, topics = basicTopics, withSchemaRegistry = true)
            val nBroker = 1
            val client = HttpClient(Apache)

            beforeGroup {
                kEnv.start()
            }

            it("should have 1 zookeeper") {

                FourLetterWordMain.send4LetterWord(
                        kEnv.serverPark.zookeeper.host,
                        kEnv.serverPark.zookeeper.port,
                        "ruok") shouldBeEqualTo "imok\n"
            }

            it("should have $nBroker broker") {

                kEnv.adminClient.describeCluster().nodes().get().toList().size shouldEqualTo nBroker
            }

            it("should have ${basicTopics.size + 1} topics available") {

                // schema registry will add __schemas topic to kafka broker
                kEnv.adminClient.listTopics().names().get().toList().size shouldEqualTo basicTopics.size + 1
            }

            it("should have topics $basicTopics  available") {

                kEnv.adminClient.listTopics().names().get().toList() shouldContainAll basicTopics
            }

            it("should report default compatibility level for schema registry") {

                runBlocking {
                    client.getSomething(URL(kEnv.serverPark.schemaregistry.url + "/config"))
                } shouldBeEqualTo SCHEMAREG_DefaultCompatibilityLevel
            }

            it("should report zero subjects for schema registry") {

                runBlocking {
                    client.getSomething(URL(kEnv.serverPark.schemaregistry.url + "/subjects"))
                } shouldBeEqualTo SCHEMAREG_NoSubjects
            }

            afterGroup {
                kEnv.tearDown()
            }
        }

        //
    }
})