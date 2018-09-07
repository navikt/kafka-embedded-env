package no.nav.common.embeddedzookeeper

import no.nav.common.KafkaEnvironment
import org.amshove.kluent.shouldBeEqualTo
import org.apache.zookeeper.client.FourLetterWordMain
import org.spekframework.spek2.Spek
import org.spekframework.spek2.style.specification.describe

object ZKServerSpec : Spek({

    val kEnv = KafkaEnvironment(0) // need only zookeeper

    describe("zookeeper server tests") {

        beforeGroup {
            // nothing here
        }

        context("active embeddedzookeeper (start/stop)") {

            beforeGroup {
                kEnv.start()
            }

            it("should be ok - command ruok with response imok") {

                FourLetterWordMain.send4LetterWord(
                        kEnv.serverPark.zookeeper.host,
                        kEnv.serverPark.zookeeper.port,
                        "ruok") shouldBeEqualTo "imok\n"
            }

            it("should have no outstanding requests - command reqs with response empty string") {

                FourLetterWordMain.send4LetterWord(
                        kEnv.serverPark.zookeeper.host,
                        kEnv.serverPark.zookeeper.port,
                        "reqs") shouldBeEqualTo ""
            }

            afterGroup {
                kEnv.stop()
            }
        }

        context("active embeddedzookeeper (start/stop for 2nd time) ") {

            beforeGroup {
                kEnv.start()
            }

            it("should be ok - command ruok with response imok") {

                FourLetterWordMain.send4LetterWord(
                        kEnv.serverPark.zookeeper.host,
                        kEnv.serverPark.zookeeper.port,
                        "ruok") shouldBeEqualTo "imok\n"
            }

            it("should have no outstanding requests - command reqs with response empty string") {

                FourLetterWordMain.send4LetterWord(
                        kEnv.serverPark.zookeeper.host,
                        kEnv.serverPark.zookeeper.port,
                        "reqs") shouldBeEqualTo ""
            }

            afterGroup {
                kEnv.stop()
            }
        }

        afterGroup {
            kEnv.tearDown()
        }
    }
})