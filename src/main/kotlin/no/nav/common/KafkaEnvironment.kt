package no.nav.common

import mu.KotlinLogging
import no.nav.common.embeddedkafka.KBServer
import no.nav.common.embeddedkafkarest.KRServer
import no.nav.common.embeddedksql.KSQLServer
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
 * @param withKSQL optional ksql server - default false
 * @param autoStart start servers immediately - default false
 *
 * withSchemaRegistry as true automatically include at least one broker
 * withKSQL as true will automatically include schema registry
 * withRest as true will automatically include schema registry
 *
 * No topics are created if only zookeeper is requested
 *
 * A [ServerPark] property is available for custom management of servers
 * A [brokersURL] property is available, expedient when multiple brokers
 * A [adminClient] property is available, referring to invalid PLAINTEXT://localhost:0000 in case of just zookeeper
 *
 */
class KafkaEnvironment(
    noOfBrokers: Int = 1,
    val topics: List<String> = emptyList(),
    withSchemaRegistry: Boolean = false,
    withKSQL: Boolean = false,
    // withRest: Boolean = false,
    autoStart: Boolean = false
) {

    private val withRest = false // TODO enable REST when no dependency to previous version

    /**
     * A server park of the configured kafka environment
     * Each server has basic properties (url, host, port)
     * and start/stop methods
     */
    data class ServerPark(
        val zookeeper: ServerBase,
        val brokers: List<ServerBase>,
        val schemaregistry: ServerBase,
        val ksql: ServerBase,
        val rest: ServerBase
    )

    // in case of strange config
    private val reqNoOfBrokers = when {
        (noOfBrokers < 1 && (withSchemaRegistry || withKSQL || withRest || topics.isNotEmpty())) -> 1
        (noOfBrokers < 1 && !(withSchemaRegistry || withKSQL || withRest || topics.isNotEmpty())) -> 0
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

    private val ksqlDir = File(System.getProperty("java.io.tmpdir"), "inmksql").apply {
        // in case of fatal failure and no deletion in previous run
        try { FileUtils.deleteDirectory(this) } catch (e: IOException) { /* tried at least */ }
    }

    val serverPark: ServerPark
    val brokersURL: String
    val adminClient: AdminClient

    // initialize servers and start, creation of topics
    init {
        val zk = ZKServer(getAvailablePort(), zkDataDir)
        val kBrokers = (0 until reqNoOfBrokers).map {
            KBServer(getAvailablePort(), it, reqNoOfBrokers, kbLDirIter.next(), zk.url)
        }

        // getting a default value in case of no brokers. Avoiding nullable adminClient management
        brokersURL = if (reqNoOfBrokers < 1) "PLAINTEXT://localhost:0000"
        else kBrokers.map { it.url }
                .foldRight("") { u, acc -> if (acc.isEmpty()) u else "$u,$acc" }

        adminClient = AdminClient.create(
                Properties().apply {
                    set(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokersURL)
                    set(ConsumerConfig.CLIENT_ID_CONFIG, "embkafka-adminclient")
                }
        )

        val sr = if (withSchemaRegistry || withKSQL || withRest)
            SRServer(getAvailablePort(), brokersURL) else EmptyShellServer()

        val ksql = if (withKSQL) KSQLServer(getAvailablePort(), brokersURL, ksqlDir.absolutePath) else EmptyShellServer()

        val r = if (withRest) KRServer(getAvailablePort(), brokersURL, sr.url) else EmptyShellServer()

        serverPark = ServerPark(zk, kBrokers, sr, ksql, r)

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
            log.info { "Eventually starting ksql server - ${ksql.url}" }
            ksql.start()
        }
        createTopics(topics)
    }

    /**
     * Stop the kafka environment
     */
    fun stop() = serverPark.apply {
        log.info { "Eventually stopping ksql server" }
        ksql.stop()
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
        try { FileUtils.deleteDirectory(ksqlDir) } catch (e: IOException) { /* tried at least */ }
    }

    // see the following link for creating topic
    // https://kafka.apache.org/20/javadoc/org/apache/kafka/clients/admin/AdminClient.html#createTopics-java.util.Collection-

    private fun createTopics(topics: List<String>) {

        if (topicsCreated || topics.isEmpty()) return

        if (!topicsCreated && serverPark.brokers.isEmpty()) {
            topicsCreated = true
            return
        }

        val noPartitions = serverPark.brokers.size
        val replFactor = serverPark.brokers.size

        adminClient.createTopics(topics.map { NewTopic(it, noPartitions, replFactor.toShort()) })

        topicsCreated = true
    }

    companion object {
        val log = KotlinLogging.logger { }
    }
}
