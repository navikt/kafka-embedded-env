package no.nav.common

import javax.security.auth.login.AppConfigurationEntry
import javax.security.auth.login.Configuration

/**
 *  An object for setting minimum JAAS context security for zookeeper and kafka broker
 *  - Server - for accessing zookeeper with given user&pwd
 *  - sasl_plaintext.KafkaServer - setting the user&pwd for kafka broker itself and users to access the broker
 *  - Client - user&pwd the kafka broker uses to access zookeeper
 *  - KafkaClient - user&pwd for e.g. schema registry
 *
 *  See code for zookeeper and kafka broker
 */

internal data class JAASCredential(val username: String, val password: String)

// internal val zookeeperAdmin = JAASCredential("admin", "admin-secret")
internal val kafkaAdmin = JAASCredential("srvkafkabroker", "kafkabroker")
internal val kafkaClient = JAASCredential("srvkafkaclient", "kafkaclient")
internal val kafkaC1 = JAASCredential("srvkafkac1", "kafkac1")
internal val kafkaP1 = JAASCredential("srvkafkap1", "kafkap1")
internal val kafkaC2 = JAASCredential("srvkafkac2", "kafkac2")
internal val kafkaP2 = JAASCredential("srvkafkap2", "kafkap2")

object JAASContext {

    const val PLAIN_LOGIN = "org.apache.kafka.common.security.plain.PlainLoginModule"
    const val DIGEST_LOGIN = "org.apache.zookeeper.server.auth.DigestLoginModule"
    val required: AppConfigurationEntry.LoginModuleControlFlag = AppConfigurationEntry.LoginModuleControlFlag.REQUIRED

    fun setUp() {

        val config = object : Configuration() {
            override fun getAppConfigurationEntry(name: String): Array<AppConfigurationEntry> =

                    when (name) {
                        // zookeeper section
                        "Server" -> arrayOf(
                                AppConfigurationEntry(
                                        DIGEST_LOGIN,
                                        required,
                                        hashMapOf<String, Any>(
                                                "username" to kafkaAdmin.username,
                                                "password" to kafkaAdmin.password,
                                                "user_${kafkaAdmin.username}" to kafkaAdmin.password
                                        )
                                )
                        )
                        // kafka server section
                        "sasl_plaintext.KafkaServer" -> arrayOf(
                                AppConfigurationEntry(
                                        PLAIN_LOGIN,
                                        required,
                                        hashMapOf<String, Any>(
                                                "username" to kafkaAdmin.username,
                                                "password" to kafkaAdmin.password,
                                                "user_${kafkaAdmin.username}" to kafkaAdmin.password,
                                                "user_${kafkaClient.username}" to kafkaClient.password,
                                                "user_${kafkaC1.username}" to kafkaC1.password,
                                                "user_${kafkaP1.username}" to kafkaP1.password,
                                                "user_${kafkaC2.username}" to kafkaC2.password,
                                                "user_${kafkaP2.username}" to kafkaP2.password
                                        )
                                )
                        )
                        // kafka server as client of zookeeper
                        "Client" -> arrayOf(
                                AppConfigurationEntry(
                                        DIGEST_LOGIN,
                                        required,
                                        hashMapOf<String, Any>(
                                                "username" to kafkaAdmin.username,
                                                "password" to kafkaAdmin.password
                                        )
                                )
                        )
                        // kafka client section, e.g. schema registry
                        "KafkaClient" -> arrayOf(
                                AppConfigurationEntry(
                                        PLAIN_LOGIN,
                                        required,
                                        hashMapOf<String, Any>(
                                                "username" to kafkaClient.username,
                                                "password" to kafkaClient.password
                                        )
                                )
                        )
                        else -> {
                            println("JAAS section name -  $name")
                            arrayOf(
                                    AppConfigurationEntry(
                                            "invalid",
                                            required,
                                            hashMapOf<String, Any>(
                                                    "username" to "invalid",
                                                    "password" to "invalid"
                                            )
                                    )
                            )
                        }
                    }

            override fun refresh() {
                // ignored
            }
        }
        // make the JAAS config available
        Configuration.setConfiguration(config)
    }
}