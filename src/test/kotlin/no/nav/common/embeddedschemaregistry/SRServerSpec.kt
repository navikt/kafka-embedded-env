package no.nav.common.embeddedschemaregistry

import io.ktor.client.HttpClient
import io.ktor.client.engine.apache.Apache
import no.nav.common.KafkaEnvironment
import no.nav.common.embeddedutils.ServerBase
import no.nav.common.test.common.httpReqResp
import no.nav.common.test.common.scRegTests
import org.amshove.kluent.shouldBeEqualTo
import org.spekframework.spek2.Spek
import org.spekframework.spek2.style.specification.describe

object SRServerSpec : Spek({

    // too much housekeeping involved starting schema registry without KafkaEnvironment

    describe("schema registry tests") {

        val client = HttpClient(Apache)

        val kEnvSRSS = KafkaEnvironment(withSchemaRegistry = true)
        var schemaReg: ServerBase? = null

        beforeGroup {
            kEnvSRSS.start()
            schemaReg = kEnvSRSS.schemaRegistry
        }

        context("active embedded kafka cluster with schema reg") {

            scRegTests.forEach { txt, cmdRes ->
                it(txt) { httpReqResp(client, schemaReg!!, cmdRes.first) shouldBeEqualTo cmdRes.second }
            }
        }

        context("active embedded kafka cluster with stopped schema reg") {

            beforeGroup { schemaReg!!.stop() }

            it("should not report config - connection refused") {
                httpReqResp(client, schemaReg!!, "/config") shouldBeEqualTo "java.net.ConnectException"
            }
        }

        context("active embedded kafka cluster with restarted schema reg") {

            beforeGroup { schemaReg!!.start() }

            scRegTests.forEach { txt, cmdRes ->
                it(txt) { httpReqResp(client, schemaReg!!, cmdRes.first) shouldBeEqualTo cmdRes.second }
            }
        }

        afterGroup { kEnvSRSS.tearDown() }
    }
})