package no.nav.common.embeddedschemaregistry

import com.github.kittinunf.fuel.httpGet
import com.github.kittinunf.result.Result
import no.nav.common.KafkaEnvironment
import org.amshove.kluent.shouldEqual
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.context
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.it

object SRServerSpec : Spek({

    val kEnv = KafkaEnvironment(1, withSchemaRegistry = true)

    describe("schema registry tests") {

        beforeGroup {
            kEnv.start()
        }

        context("active embeddedkafka cluster with schema reg") {

            it("should report config compatibility level") {

                (kEnv.serverPark.schemaregistry.url + "/config")
                        .httpGet()
                        .responseString().third.component1() shouldEqual """{"compatibilityLevel":"BACKWARD"}"""
            }

            it("should report zero subjects") {

                (kEnv.serverPark.schemaregistry.url + "/subjects")
                        .httpGet()
                        .responseString().third.component1() shouldEqual """[]"""
            }
        }

        context("active embeddedkafka cluster with stopped schema reg") {

            beforeGroup {
                kEnv.serverPark.schemaregistry.stop()
            }

            it("should not report config - connection refused") {

                val (_,_,result) = (kEnv.serverPark.schemaregistry.url + "/config").httpGet()
                        .timeout(500)
                        .responseString()

                when (result) {
                    is Result.Failure -> true //result.error.message
                    is Result.Success -> false//result.value
                } shouldEqual true//"java.net.ConnectException: Connection refused (Connection refused)"
            }

        }

        context("active embeddedkafka cluster with restarted schema reg") {

            beforeGroup {
                kEnv.serverPark.schemaregistry.start()
            }

            it("should report config compatibility level") {

                (kEnv.serverPark.schemaregistry.url + "/config")
                        .httpGet()
                        .responseString().third.component1() shouldEqual """{"compatibilityLevel":"BACKWARD"}"""
            }

            it("should report zero subjects") {

                (kEnv.serverPark.schemaregistry.url + "/subjects")
                        .httpGet()
                        .responseString().third.component1() shouldEqual """[]"""
            }
        }

        afterGroup {
            kEnv.tearDown()
        }
    }
})