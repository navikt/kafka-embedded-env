package no.nav.common

import no.nav.common.embeddedkafka.KBServer
import no.nav.common.embeddedschemaregistry.SRServer
import no.nav.common.embeddedutils.ServerBase
import no.nav.common.embeddedutils.getAvailablePort
import no.nav.common.embeddedzookeeper.ZKServer
import org.apache.commons.io.FileUtils
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.config.SaslConfigs
import java.io.File
import java.io.IOException
import java.util.Properties
import java.util.UUID

/**
 * A in-memory kafka environment consisting of
 * - 1 zookeeper
 * @param noOfBrokers no of brokers to spin up, default one and maximum 2
 * @param topics a list of topics to create at environment startup - default empty
 * @param withSchemaRegistry optional schema registry - default false
 * @param autoStart start servers immediately - default false
 * @param withSecurity gives SASL plain security (authentication and authorization)
 * @param users add custom users for authentication and authorization. Only relevant when withSecurity enabled
 *
 * If noOfBrokers is zero, non-empty topics or withSchemaRegistry as true, will automatically include one broker
 *
 * A [serverPark] property is available for custom management of servers
 *
 * Observe that serverPark is 'cumbersome' due to different configuration options,
 * with or without brokers and schema registry. AdminClient is depended on started brokers and so on
 *
 * Some helper properties are available in order to ease the state management. Observe(!) values depend on state
 * [zookeeper] property
 * [brokers] property - relevant iff broker(s) included in config
 * [brokersURL] property - relevant iff broker(s) included in config
 * [adminClient] property - relevant iff broker(s) included in config. and serverPark started
 * [schemaRegistry] property - relevant iff schema reg included in config and serverPark started
 *
 */

class KafkaEnvironment(
    noOfBrokers: Int = 1,
    val topics: List<String> = emptyList(),
    withSchemaRegistry: Boolean = false,
    val withSecurity: Boolean = false,
    users: List<JAASCredential> = emptyList(),
    autoStart: Boolean = false
) : AutoCloseable {

    sealed class BrokerStatus {
        data class Available(
            val brokers: List<ServerBase>,
            val brokersURL: String
        ) : BrokerStatus()
        object NotAvailable : BrokerStatus()
    }

    private fun BrokerStatus.start() = when (this) {
        is BrokerStatus.Available -> this.brokers.forEach { it.start() }
        else -> {}
    }

    private fun BrokerStatus.stop() = when (this) {
        is BrokerStatus.Available -> this.brokers.forEach { it.stop() }
        else -> {}
    }

    private fun BrokerStatus.getBrokers(): List<ServerBase> = when (this) {
        is BrokerStatus.Available -> this.brokers
        else -> emptyList()
    }

    private fun BrokerStatus.getBrokersURL(): String = when (this) {
        is BrokerStatus.Available -> this.brokersURL
        else -> ""
    }

    private fun BrokerStatus.createAdminClient(): AdminClient? = when (this) {
        is BrokerStatus.Available ->
            if (serverPark.status is ServerParkStatus.Started)
                AdminClient.create(
                        Properties().apply {
                            set(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokersURL)
                            set(ConsumerConfig.CLIENT_ID_CONFIG, "embkafka-adminclient")
                            if (withSecurity) {
                                set(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT")
                                set(SaslConfigs.SASL_MECHANISM, "PLAIN")
                                set(SaslConfigs.SASL_JAAS_CONFIG, "$JAAS_PLAIN_LOGIN $JAAS_REQUIRED " +
                                        "username=\"${kafkaClient.username}\" password=\"${kafkaClient.password}\";")
                            }
                        }
                )
            else null
        else -> null
    }

    sealed class SchemaRegistryStatus {
        data class Available(val schemaRegistry: ServerBase) : SchemaRegistryStatus()
        object NotAvailable : SchemaRegistryStatus()
    }

    private fun SchemaRegistryStatus.start() = when (this) {
        is SchemaRegistryStatus.Available -> this.schemaRegistry.start()
        else -> {}
    }

    private fun SchemaRegistryStatus.stop() = when (this) {
        is SchemaRegistryStatus.Available -> this.schemaRegistry.stop()
        else -> {}
    }

    private fun SchemaRegistryStatus.getSchemaReg(): ServerBase? = when (this) {
        is SchemaRegistryStatus.Available -> this.schemaRegistry
        else -> null
    }

    sealed class ServerParkStatus {
        object Initialized : ServerParkStatus()
        object Started : ServerParkStatus()
        object Stopped : ServerParkStatus()
        object TearDownCompleted : ServerParkStatus()
    }

    data class ServerPark(
        val zookeeper: ServerBase,
        val brokerStatus: BrokerStatus,
        val schemaRegStatus: SchemaRegistryStatus,
        val status: ServerParkStatus
    )

    // in case of strange config
    private val reqNoOfBrokers = when {
        (noOfBrokers < 1 && (withSchemaRegistry || topics.isNotEmpty())) -> 1
        (noOfBrokers < 1 && !(withSchemaRegistry || topics.isNotEmpty())) -> 0
        noOfBrokers > 2 -> 2
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

    var serverPark: ServerPark
        private set

    // initialize servers and start, creation of topics
    init {
        if (withSecurity) {
            JAASCustomUsers.addUsers(users)
            setUpJAASContext()
        }

        val zk = ZKServer(getAvailablePort(), zkDataDir, withSecurity)

        val kBrokers = (0 until reqNoOfBrokers).map {
            KBServer(getAvailablePort(), it, reqNoOfBrokers, kbLDirIter.next(), zk.url, withSecurity)
        }
        val brokersURL = kBrokers.map { it.url }.foldRight("") { u, acc ->
            if (acc.isEmpty()) u else "$u,$acc"
        }

        serverPark = ServerPark(
                zk,
                when (reqNoOfBrokers) {
                    0 -> BrokerStatus.NotAvailable
                    else -> BrokerStatus.Available(kBrokers, brokersURL)
                },
                when (withSchemaRegistry) {
                    false -> SchemaRegistryStatus.NotAvailable
                    else -> SchemaRegistryStatus.Available(SRServer(getAvailablePort(), brokersURL, withSecurity))
                },
                ServerParkStatus.Initialized
        )

        if (autoStart) start()
    }

    // ease of state management by properties
    val zookeeper get() = serverPark.zookeeper as ZKServer
    val brokers get() = serverPark.brokerStatus.getBrokers()
    val brokersURL get() = serverPark.brokerStatus.getBrokersURL()
    val adminClient get() = serverPark.brokerStatus.createAdminClient()
    val schemaRegistry get() = serverPark.schemaRegStatus.getSchemaReg()

    /**
     * Start the kafka environment
     */
    fun start() {

        when (serverPark.status) {
            is ServerParkStatus.Started -> return
            is ServerParkStatus.TearDownCompleted -> return
            else -> {}
        }

        serverPark = serverPark.let { sp ->
            sp.zookeeper.start()
            sp.brokerStatus.start()
            sp.schemaRegStatus.start()

            ServerPark(sp.zookeeper, sp.brokerStatus, sp.schemaRegStatus, ServerParkStatus.Started)
        }

        if (serverPark.brokerStatus is BrokerStatus.Available && topics.isNotEmpty() && !topicsCreated)
            createTopics(topics)
    }

    /**
     * Stop the kafka environment
     */
    private fun stop() {

        when (serverPark.status) {
            is ServerParkStatus.Stopped -> return
            is ServerParkStatus.TearDownCompleted -> return
            else -> {}
        }

        serverPark = serverPark.let { sp ->
            sp.schemaRegStatus.stop()
            sp.brokerStatus.stop()
            sp.zookeeper.stop()

            ServerPark(sp.zookeeper, sp.brokerStatus, sp.schemaRegStatus, ServerParkStatus.Stopped)
        }
    }

    /**
     * Tear down the kafka environment, removing all data created in environment
     */
    fun tearDown() {

        when (serverPark.status) {
            is ServerParkStatus.TearDownCompleted -> return
            is ServerParkStatus.Started -> stop()
            else -> {}
        }

        try { FileUtils.deleteDirectory(zkDataDir) } catch (e: IOException) { /* tried at least */ }
        try { FileUtils.deleteDirectory(kbLDirRoot) } catch (e: IOException) { /* tried at least */ }

        serverPark = ServerPark(
                serverPark.zookeeper,
                BrokerStatus.NotAvailable,
                SchemaRegistryStatus.NotAvailable,
                ServerParkStatus.TearDownCompleted
        )
    }

    override fun close() { tearDown() }

    // see the following link for creating topic
    // https://kafka.apache.org/20/javadoc/org/apache/kafka/clients/admin/AdminClient.html#createTopics-java.util.Collection-

    private fun createTopics(topics: List<String>) {

        // this func is only invoked if broker(s) are available and started

        val noPartitions = this.brokers.size
        val replFactor = this.brokers.size

        this.adminClient?.use { ac ->
            ac.createTopics(topics.map { name -> NewTopic(name, noPartitions, replFactor.toShort()) })
        }

        topicsCreated = true
    }
}
