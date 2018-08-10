package no.nav.common

import mu.KotlinLogging
import no.nav.common.embeddedkafka.KBServer
import no.nav.common.embeddedkafkarest.KRServer
import no.nav.common.embeddedschemaregistry.SRServer
import no.nav.common.embeddedutils.ServerBase
import no.nav.common.embeddedutils.EmptyShellServer
import no.nav.common.embeddedutils.getAvailablePort
import no.nav.common.embeddedzookeeper.ZKServer
import org.apache.commons.io.FileUtils
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import java.io.File
import java.io.IOException
import java.util.Properties
import java.util.UUID

/**
 * A in-memory kafka environment consisting of
 * - 1 zookeeper
 * @param noOfBrokers no of brokers to spin up, default one
 * @param topics a list of topics to create at environment startup - default empty
 * @param withSchemaRegistry optional schema registry - default false
 * @param withRest optional rest server - default false
 * @param autoStart start servers immediately - default false
 *
 * withRest as true includes automatically schema registry
 * schema registry includes automatically at least one broker
 *
 * No topics are created if only zookeeper is requested
 *
 * A [ServerPark] property is available for custom management of servers
 * A [brokersURL] property is available, expedient when multiple brokers
 *
 */
class KafkaEnvironment(
    private val noOfBrokers: Int = 1,
    val topics: List<String> = emptyList(),
    withSchemaRegistry: Boolean = false,
    withRest: Boolean = false,
    autoStart: Boolean = false
) {

    /**
     * A server park of the configured kafka environment
     * Each server has basic properties (url, host, port)
     * and start/stop methods
     */
    data class ServerPark(
        val zookeeper: ServerBase,
        val brokers: List<ServerBase>,
        val schemaregistry: ServerBase,
        val rest: ServerBase
    )

    // in case of strange config
    private val reqNoOfBrokers = when {
        (noOfBrokers < 1 && (withSchemaRegistry || withRest)) -> 1
        (noOfBrokers < 0 && !(withSchemaRegistry || withRest)) -> 0
        else -> noOfBrokers
    }

    // in case of start of environment will be manually triggered later on
    private var topicsCreated = false

    private val zkDataDir = File(System.getProperty("java.io.tmpdir"), "inmzookeeper").apply {
        // in case of fatal failure and no deletion in previous run
        try { FileUtils.deleteDirectory(this) } catch (e: IOException) { /* tried at least */ }
    }

    private val kbLDirRoot = File(System.getProperty("java.io.tmpdir"), "inmkafkabroker").apply {
        // in case of fatal failure and no deletion in previous run
        try { FileUtils.deleteDirectory(this) } catch (e: IOException) { /* tried at least */ }
    }
    private val kbLDirIter = (0 until reqNoOfBrokers).map {
        File(System.getProperty("java.io.tmpdir"), "inmkafkabroker/ID$it${UUID.randomUUID()}")
    }.iterator()

    // allocate enough available ports
    private val noOfPorts = 1 + reqNoOfBrokers +
            listOf((withSchemaRegistry || withRest), withRest).filter { it == true }.size

    private val portsIter = (1..noOfPorts).map { getAvailablePort() }.iterator()

    val serverPark: ServerPark
    val brokersURL: String

    // initialize servers and start, creation of topics
    init {
        val zk = ZKServer(portsIter.next(), zkDataDir)
        val kBrokers = (0 until reqNoOfBrokers).map {
            KBServer(portsIter.next(), it, reqNoOfBrokers, kbLDirIter.next(), zk.url)
        }
        brokersURL = kBrokers.map { it.url }
                .foldRight("") { u, acc -> if (acc.isEmpty()) u else "$u,$acc" }

        val sr = if (withSchemaRegistry || withRest) SRServer(portsIter.next(), zk.url, brokersURL) else EmptyShellServer()
        val r = if (withRest) KRServer(portsIter.next(), zk.url, brokersURL, sr.url) else EmptyShellServer()

        serverPark = ServerPark(zk, kBrokers, sr, r)

        if (autoStart) start()
    }

    /**
     * Start the kafka environment
     */
    fun start() {
        serverPark.apply {
            log.info { "Starting zookeeper - ${zookeeper.url}" }
            zookeeper.start()
            log.info { "Eventually starting kafka broker(s) - $brokersURL" }
            brokers.forEach { it.start() }
            log.info { "Eventually  starting schema registry - ${schemaregistry.url}" }
            schemaregistry.start()
            log.info { "Eventually starting rest server - ${rest.url}" }
            rest.start()
        }
        createTopics(topics)
    }

    /**
     * Stop the kafka environment
     */
    fun stop() = serverPark.apply {
        log.info { "Eventually stopping rest server" }
        rest.stop()
        log.info { "Eventually stopping schema registry" }
        schemaregistry.stop()
        log.info { "Eventually stopping kafka broker(s)" }
        brokers.forEach { it.stop() }
        log.info { "Stopping zookeeper" }
        zookeeper.stop()
    }

    /**
     * Tear down the kafka environment, removing all data created in environment session
     */
    fun tearDown() {
        stop()
        try { FileUtils.deleteDirectory(zkDataDir) } catch (e: IOException) { /* tried at least */ }
        try { FileUtils.deleteDirectory(kbLDirRoot) } catch (e: IOException) { /* tried at least */ }
    }

    // see the following link for creating topic
    // https://kafka.apache.org/20/javadoc/org/apache/kafka/clients/admin/AdminClient.html#createTopics-java.util.Collection-

    private fun createTopics(topics: List<String>) {

        if (topicsCreated || topics.isEmpty()) return

        if (!topicsCreated && serverPark.brokers.isEmpty()) {
            topicsCreated = true
            return
        }

        AdminClient.create(
                Properties().apply {
                    set(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokersURL)
                    set(ConsumerConfig.CLIENT_ID_CONFIG, "embkafka-adminclient")
                }
        ).use { adminClient ->

            val noPartitions = serverPark.brokers.size
            val replFactor = serverPark.brokers.size

            adminClient.createTopics(topics.map { NewTopic(it, noPartitions, replFactor.toShort()) })
        }

        topicsCreated = true
    }

    companion object {
        val log = KotlinLogging.logger { }
    }
}
