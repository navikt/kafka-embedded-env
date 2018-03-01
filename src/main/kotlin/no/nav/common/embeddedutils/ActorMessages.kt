package no.nav.common.embeddedutils

/**
 * A list of commands to onReceive
 */

sealed class ServerMessages

// zookeeper start/stop
object ZKStart : ServerMessages()
object ZKStop : ServerMessages()

// kafka broker(s) start/stop
class KBStart(val noOfBrokers: Int = 1) : ServerMessages()
object KBStop : ServerMessages()

// schema registry start/stop
object SRStart : ServerMessages()
object SRStop : ServerMessages()

// kafka rest start/stop
object KRStart : ServerMessages()
object KRStop : ServerMessages()


