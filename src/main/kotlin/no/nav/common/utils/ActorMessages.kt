package no.nav.common.utils

sealed class ServerMessages
object ZKStart : ServerMessages()
object ZKStop : ServerMessages()
class KBStart(val noOfBrokers: Int = 1) : ServerMessages()
object KBStop : ServerMessages()

