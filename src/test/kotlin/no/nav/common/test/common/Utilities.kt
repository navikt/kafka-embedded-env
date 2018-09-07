package no.nav.common.test.common

import com.nhaarman.mockito_kotlin.timeout
import io.ktor.client.HttpClient
import io.ktor.client.request.get
import io.ktor.client.request.url
import io.ktor.http.ContentType
import io.ktor.http.contentType
import java.net.URL

const val SCHEMAREG_DefaultCompatibilityLevel = """{"compatibilityLevel":"BACKWARD"}"""
val SCHEMAREG_NoSubjects = """[]"""

suspend fun HttpClient.getSomething(endpoint: URL): String =
        this.get {
            url(endpoint)
            contentType(ContentType.Application.Json)
            timeout(500)
        }
