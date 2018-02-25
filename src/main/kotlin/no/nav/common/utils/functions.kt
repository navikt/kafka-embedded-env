package no.nav.common.utils

import java.io.IOException
import java.net.ServerSocket

fun getAvailablePort(): Int =
        try{
            ServerSocket(0).run {
                reuseAddress = true
                close()
                localPort
            }
        }
        catch (e: IOException) {0} //TODO - watch out for this one