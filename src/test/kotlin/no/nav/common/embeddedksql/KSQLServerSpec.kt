package no.nav.common.embeddedksql

import com.nhaarman.mockito_kotlin.timeout
import io.ktor.client.HttpClient
import io.ktor.client.engine.apache.Apache
import io.ktor.client.request.post
import io.ktor.client.request.url
import io.ktor.http.ContentType
import io.ktor.http.contentType
import kotlinx.coroutines.experimental.runBlocking
import no.nav.common.KafkaEnvironment
import org.amshove.kluent.shouldBeEqualTo
import org.spekframework.spek2.Spek
import org.spekframework.spek2.style.specification.describe
import java.net.URL

object KSQLServerSpec : Spek({

    val kEnv = KafkaEnvironment(1, withKSQL = true)
    val client = HttpClient(Apache)

    suspend fun getStreams(): String =

        client.post {
            url(URL(kEnv.serverPark.ksql.url + "/ksql"))
            contentType(ContentType.Application.Json)
            timeout(500)
            body = """{"ksql": "LIST STREAMS;","streamsProperties": {}}"""
        }

    describe("ksql tests") {

        context("active embeddedkafka cluster") {

            beforeGroup {
                kEnv.start()
            }

            it("should have ksql server available") {

                val respons = runBlocking { getStreams() }

                respons shouldBeEqualTo """[{"@type":"streams","statementText":"LIST STREAMS;","streams":[]}]"""
            }

            afterGroup {
                kEnv.tearDown()
            }
        }
    }
})