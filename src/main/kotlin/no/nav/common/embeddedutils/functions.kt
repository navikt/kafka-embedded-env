package no.nav.common.embeddedutils

import java.io.IOException
import java.net.ServerSocket

/**
 * A function returning the next available socket port
 */
fun getAvailablePort(): Int =
        try {
            ServerSocket(0).run {
                reuseAddress = true
                close()
                localPort
            }
        } catch (e: IOException) { 0 } // TODO - watch out for this one