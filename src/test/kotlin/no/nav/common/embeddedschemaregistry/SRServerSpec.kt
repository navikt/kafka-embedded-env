package no.nav.common.embeddedschemaregistry

import no.nav.common.embeddedkafka.KBServer
import no.nav.common.embeddedutils.*
import no.nav.common.embeddedzookeeper.ZKServer
import org.amshove.kluent.`should be equal to`
import org.amshove.kluent.`should not be equal to`
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.it

object SRServerSpec : Spek({

    describe("active embeddedkafka cluster of one broker with schema reg (start/stop)") {

        val b = 1

        beforeGroup {

            ZKServer.onReceive(ZKStart)
            KBServer.onReceive(KBStart(b))
            SRServer.onReceive(SRStart)
        }

        it("should have a schema reg with port different from 0") {

            SRServer.getPort() `should not be equal to` 0
        }

        afterGroup {

            SRServer.onReceive(SRStop)
            KBServer.onReceive(KBStop)
            ZKServer.onReceive(ZKStop)
        }

    }

    describe("inactive embeddedkafka cluster and no schema reg (no start/stop)") {

        beforeGroup {  }

        it("should return empty string as host") {
            SRServer.getHost() `should be equal to` ""
        }

        it("should return 0 as port") {
            SRServer.getPort() `should be equal to` 0
        }

        it("should return empty string as url") {
            SRServer.getUrl() `should be equal to` ""
        }

        afterGroup {  }
    }
})