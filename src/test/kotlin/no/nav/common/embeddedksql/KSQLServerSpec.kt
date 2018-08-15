package no.nav.common.embeddedksql

import com.github.kittinunf.fuel.httpPost
import no.nav.common.KafkaEnvironment
import org.amshove.kluent.shouldEqualTo
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.context
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.it

object KSQLServerSpec : Spek({

    val kEnv = KafkaEnvironment(1, withKSQL = true)

    describe("ksql tests") {

        context("active embeddedkafka cluster") {

            beforeGroup {
                kEnv.start()
            }

            it("should have ksql server available") {

                val x = (kEnv.serverPark.ksql.url + "/ksql")
                        .httpPost()
                        .header(Pair("Content-Type", "application/vnd.ksql.v1+json; charset=utf-8"))
                        .body("""{"ksql": "LIST STREAMS;","streamsProperties": {}}""".trimMargin())
                        .responseString()
                        .third
                        .component1() // shouldEqual """[{"@type": "streams","statementText": "LIST STREAMS;","streams": []}]"""

                1 shouldEqualTo 1
            }

            afterGroup {
                kEnv.tearDown()
            }
        }
    }
})