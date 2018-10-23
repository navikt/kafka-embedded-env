package no.nav.common.test.common

import com.nhaarman.mockito_kotlin.timeout
import io.ktor.client.HttpClient
import io.ktor.client.request.get
import io.ktor.client.request.url
import io.ktor.http.ContentType
import io.ktor.http.contentType
import kotlinx.coroutines.experimental.runBlocking
import no.nav.common.embeddedutils.ServerBase
import org.apache.kafka.clients.admin.AdminClient
import java.net.URL

// some kafka broker test utilities

val noOfBrokers: (AdminClient) -> Int = { it.describeCluster().nodes().get().toList().size }
val noOfTopics: (AdminClient) -> Int = { it.listTopics().names().get().toList().size }
val topics: (AdminClient) -> List<String> = { it.listTopics().names().get().toList() }

// some schema registry test utilities

const val SCHEMAREG_DefaultCompatibilityLevel = """{"compatibilityLevel":"BACKWARD"}"""
const val SCHEMAREG_NoSubjects = """[]"""

val scRegTests = mapOf(
        "should report default compatibility level" to Pair("/config", SCHEMAREG_DefaultCompatibilityLevel),
        "should report zero subjects" to Pair("/subjects", SCHEMAREG_NoSubjects)
)

suspend fun HttpClient.getSomething(endpoint: URL): String =
        this.get {
            url(endpoint)
            contentType(ContentType.Application.Json)
            timeout(500)
        }

val httpReqResp: (HttpClient, ServerBase, String) -> String = { client, sr, path ->
    try {
        runBlocking { client.getSomething(URL(sr.url + path)) }
    } catch (e: Exception) {
        e.javaClass.name
    }
}
