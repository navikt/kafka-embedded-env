package no.nav.common.embeddedschemaregistry

import com.nhaarman.mockito_kotlin.timeout
import io.ktor.client.HttpClient
import io.ktor.client.engine.apache.Apache
import io.ktor.client.request.get
import io.ktor.client.request.url
import io.ktor.http.ContentType
import io.ktor.http.contentType
import kotlinx.coroutines.experimental.runBlocking
import no.nav.common.KafkaEnvironment
import org.amshove.kluent.shouldBeEqualTo
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.context
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.it
import java.net.URL

object SRServerSpec : Spek({

    val kEnv = KafkaEnvironment(withSchemaRegistry = true)
    val client = HttpClient(Apache)

    val defaultCompatibilityLevel = """{"compatibilityLevel":"BACKWARD"}"""
    val noSubjects = """[]"""

    suspend fun getSomething(endpoint: String): String =
            client.get {
                url(URL(kEnv.serverPark.schemaregistry.url + "/$endpoint"))
                contentType(ContentType.Application.Json)
                timeout(500)
            }

    describe("schema registry tests") {

        beforeGroup {
            kEnv.start()
        }

        context("active embeddedkafka cluster with schema reg") {

            it("should report default compatibility level") {

                runBlocking { getSomething("config") } shouldBeEqualTo defaultCompatibilityLevel
            }

            it("should report zero subjects") {

                runBlocking { getSomething("subjects") } shouldBeEqualTo noSubjects
            }
        }

        context("active embeddedkafka cluster with stopped schema reg") {

            beforeGroup {
                kEnv.serverPark.schemaregistry.stop()
            }

            it("should not report config - connection refused") {

                val response = try {
                    runBlocking { getSomething("config") }
                } catch (e: Exception) {
                    e.javaClass.name
                }

                response shouldBeEqualTo "java.net.ConnectException"
            }
        }

        context("active embeddedkafka cluster with restarted schema reg") {

            beforeGroup {
                kEnv.serverPark.schemaregistry.start()
            }

            it("should report config compatibility level") {

                runBlocking { getSomething("config") } shouldBeEqualTo defaultCompatibilityLevel
            }

            it("should report zero subjects") {

                runBlocking { getSomething("subjects") } shouldBeEqualTo noSubjects
            }
        }

        afterGroup {
            kEnv.tearDown()
        }
    }
})