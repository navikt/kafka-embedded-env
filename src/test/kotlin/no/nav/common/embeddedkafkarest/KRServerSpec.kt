package no.nav.common.embeddedkafkarest

import com.github.kittinunf.fuel.httpGet
import com.github.kittinunf.result.Result
import no.nav.common.KafkaEnvironment
import org.amshove.kluent.shouldEqual
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.context
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.xdescribe

object KRServerSpec : Spek({

    // TODO - kafka rest is disabled - see KRServer
    val kEnv = KafkaEnvironment(/*withRest = true*/)

    xdescribe("kafka rest tests") {

        beforeGroup {
            kEnv.start()
        }

        context("active embeddedkafka cluster with rest") {

            val b = 1

            it("should report none topics via rest") {

                // quick and raw http&json

                (kEnv.serverPark.rest.url + "/topics")
                        .httpGet()
                        .responseString().third.component1() shouldEqual """["_schemas"]"""
            }

            it("should report $b broker via rest") {

                // quick and raw http&json

                (kEnv.serverPark.rest.url + "/brokers")
                        .httpGet()
                        .responseString().third.component1() shouldEqual """{"brokers":[0]}"""
            }
        }

        context("active embeddedkafka cluster with stopped rest") {

            beforeGroup {
                kEnv.serverPark.rest.stop()
            }

            it("should not report brokers - connection refused") {

                val (_, _, result) = (kEnv.serverPark.rest.url + "/brokers").httpGet()
                        .timeout(500)
                        .responseString()

                when (result) {
                    is Result.Failure -> true // result.error.message
                    is Result.Success -> false // result.value
                } shouldEqual true // "java.net.ConnectException: Connection refused (Connection refused)"
            }
        }

        context("active embeddedkafka cluster with restarted rest") {

            beforeGroup {
                kEnv.serverPark.rest.start()
            }

            val b = 1

            it("should report none topics via rest") {

                // quick and raw http&json

                (kEnv.serverPark.rest.url + "/topics")
                        .httpGet()
                        .responseString().third.component1() shouldEqual """["_schemas"]"""
            }

            it("should report $b broker via rest") {

                // quick and raw http&json

                (kEnv.serverPark.rest.url + "/brokers")
                        .httpGet()
                        .responseString().third.component1() shouldEqual """{"brokers":[0]}"""
            }
        }

        afterGroup {
            kEnv.tearDown()
        }
    }
})