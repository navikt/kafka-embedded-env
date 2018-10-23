package no.nav.common.embeddedutils

/**
 * A list of commands to onReceive
 */

sealed class ServerStatus {
    object Running : ServerStatus()
    object NotRunning : ServerStatus()
}
