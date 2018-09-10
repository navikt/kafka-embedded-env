package no.nav.common.embeddedschemaregistry

import io.ktor.client.HttpClient
import io.ktor.client.engine.apache.Apache
import kotlinx.coroutines.experimental.runBlocking
import no.nav.common.KafkaEnvironment
import no.nav.common.test.common.SCHEMAREG_DefaultCompatibilityLevel
import no.nav.common.test.common.SCHEMAREG_NoSubjects
import no.nav.common.test.common.getSomething
import org.amshove.kluent.shouldBeEqualTo
import org.spekframework.spek2.Spek
import org.spekframework.spek2.style.specification.describe
import java.net.URL

object SRServerSpec : Spek({

    describe("schema registry tests") {

        val kEnvSRSS = KafkaEnvironment(withSchemaRegistry = true)
        val client = HttpClient(Apache)

        beforeGroup {
            kEnvSRSS.start()
        }

        context("active embeddedkafka cluster with schema reg") {

            it("should report default compatibility level") {

                runBlocking {
                    client.getSomething(URL(kEnvSRSS.serverPark.schemaregistry.url + "/config"))
                } shouldBeEqualTo SCHEMAREG_DefaultCompatibilityLevel
            }

            it("should report zero subjects") {

                runBlocking {
                    client.getSomething(URL(kEnvSRSS.serverPark.schemaregistry.url + "/subjects"))
                } shouldBeEqualTo SCHEMAREG_NoSubjects
            }
        }

        context("active embeddedkafka cluster with stopped schema reg") {

            beforeGroup {
                kEnvSRSS.serverPark.schemaregistry.stop()
            }

            it("should not report config - connection refused") {

                val response = try {
                    runBlocking {
                        client.getSomething(URL(kEnvSRSS.serverPark.schemaregistry.url + "/config"))
                    }
                } catch (e: Exception) {
                    e.javaClass.name
                }

                response shouldBeEqualTo "java.net.ConnectException"
            }
        }

        context("active embeddedkafka cluster with restarted schema reg") {

            beforeGroup {
                kEnvSRSS.serverPark.schemaregistry.start()
            }

            it("should report default compatibility level") {

                runBlocking {
                    client.getSomething(URL(kEnvSRSS.serverPark.schemaregistry.url + "/config"))
                } shouldBeEqualTo SCHEMAREG_DefaultCompatibilityLevel
            }

            it("should report zero subjects") {

                runBlocking {
                    client.getSomething(URL(kEnvSRSS.serverPark.schemaregistry.url + "/subjects"))
                } shouldBeEqualTo SCHEMAREG_NoSubjects
            }
        }

        afterGroup {
            kEnvSRSS.tearDown()
        }
    }
})