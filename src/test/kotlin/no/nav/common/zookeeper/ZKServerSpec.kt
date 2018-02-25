package no.nav.common.zookeeper

import no.nav.common.utils.ZKStart
import no.nav.common.utils.ZKStop
import org.amshove.kluent.`should be equal to`
import org.apache.zookeeper.client.FourLetterWordMain
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.it

object ZKServerSpec : Spek({

  describe("active zookeeper (start/stop)") {

      beforeGroup {
          ZKServer.onReceive(ZKStart)
      }

      it("should be ok - command ruok with response imok") {

          FourLetterWordMain.send4LetterWord(
                  ZKServer.getHost(),
                  ZKServer.getPort(),
                  "ruok") `should be equal to` "imok\n"
      }

      it("should have no outstanding requests - command reqs with response empty string") {

          FourLetterWordMain.send4LetterWord(
                  ZKServer.getHost(),
                  ZKServer.getPort(),
                  "reqs") `should be equal to` ""
      }

      afterGroup {
          ZKServer.onReceive(ZKStop)
      }
  }

    describe("active zookeeper (start/stop for 2nd time) ") {

        beforeGroup {
            ZKServer.onReceive(ZKStart)
        }

        it("should be ok - command ruok with response imok") {

            FourLetterWordMain.send4LetterWord(
                    ZKServer.getHost(),
                    ZKServer.getPort(),
                    "ruok") `should be equal to` "imok\n"
        }

        it("should have no outstanding requests - command reqs with response empty string") {

            FourLetterWordMain.send4LetterWord(
                    ZKServer.getHost(),
                    ZKServer.getPort(),
                    "reqs") `should be equal to` ""
        }

        afterGroup {
            ZKServer.onReceive(ZKStop)
        }
    }

    describe("inactive zookeeper (no start/stop)") {

        beforeGroup {  }

        it("should return empty string as host") {
            ZKServer.getHost() `should be equal to` ""
        }

        it("should return 0 as port") {
            ZKServer.getPort() `should be equal to` 0
        }

        it("should return empty string as url") {
            ZKServer.getUrl() `should be equal to` ""
        }

        afterGroup {  }
    }

    describe("active zookeeper with multiple (start/stop) ") {

        beforeGroup {
            ZKServer.onReceive(ZKStart)
            ZKServer.onReceive(ZKStart)
            ZKServer.onReceive(ZKStart)
        }

        it("should be ok - command ruok with response imok") {

            FourLetterWordMain.send4LetterWord(
                    ZKServer.getHost(),
                    ZKServer.getPort(),
                    "ruok") `should be equal to` "imok\n"
        }

        it("should have no outstanding requests - command reqs with response empty string") {

            FourLetterWordMain.send4LetterWord(
                    ZKServer.getHost(),
                    ZKServer.getPort(),
                    "reqs") `should be equal to` ""
        }

        afterGroup {
            ZKServer.onReceive(ZKStop)
            ZKServer.onReceive(ZKStop)
            ZKServer.onReceive(ZKStop)
        }
    }
})