package no.nav.common

import no.nav.common.kafka.KBServer
import no.nav.common.zookeeper.ZKServer
import java.util.*

fun main(args: Array<String>) {

    KafkaEnvironment.start(3, listOf("test1"))

    println("zookeeper: ${ZKServer.getUrl()}")
    println("kafka brokers: ${KBServer.getUrl()}")

    val s = Scanner(System.`in`)
    print("Enter nr:")
    s.nextInt()

    KafkaEnvironment.stop()


}