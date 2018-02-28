package no.nav.common.embeddedkafkarest

import com.github.kittinunf.fuel.httpGet
import no.nav.common.embeddedkafka.KBServer
import no.nav.common.embeddedschemaregistry.SRServer
import no.nav.common.embeddedutils.*
import no.nav.common.embeddedzookeeper.ZKServer
import org.amshove.kluent.`should be equal to`
import org.amshove.kluent.`should not be equal to`
import org.amshove.kluent.shouldEqual
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.it

object KRServerSpec : Spek({

    describe("active embeddedkafka cluster of one broker with rest (start/stop)") {

        val b = 1

        beforeGroup {

            ZKServer.onReceive(ZKStart)
            KBServer.onReceive(KBStart(b))
            SRServer.onReceive(SRStart)
            KRServer.onReceive(KRStart)
        }

        it("should have a rest with port different from 0") {

            KRServer.getPort() `should not be equal to` 0
        }

        afterGroup {

            KRServer.onReceive(KRStop)
            SRServer.onReceive(SRStop)
            KBServer.onReceive(KBStop)
            ZKServer.onReceive(ZKStop)
        }

        it("should report schema reg topic via rest") {

            // quick and raw http&json

            (KRServer.getUrl() + "/topics")
                    .httpGet()
                    .responseString().third.component1() shouldEqual "[\"_schemas\"]"
        }

        it("should report $b broker via rest") {

            // quick and raw http&json

            (KRServer.getUrl() + "/brokers")
                    .httpGet()
                    .responseString().third.component1() shouldEqual "{\"brokers\":[0]}"
        }

    }

    describe("inactive embeddedkafka cluster and no rest (no start/stop)") {

        beforeGroup {  }

        it("should return empty string as host") {
            KRServer.getHost() `should be equal to` ""
        }

        it("should return 0 as port") {
            KRServer.getPort() `should be equal to` 0
        }

        it("should return empty string as url") {
            KRServer.getUrl() `should be equal to` ""
        }

        afterGroup {  }
    }

})