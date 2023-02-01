[![Build Status](https://travis-ci.org/navikt/kafka-embedded-env.svg?branch=master)](https://travis-ci.org/navikt/kafka-embedded-env) 
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/no.nav/kafka-embedded-env/badge.svg)](https://maven-badges.herokuapp.com/maven-central/no.nav/kafka-embedded-env)

# kafka-embedded-env 

A simple API for creating an embedded Kafka environment with the KafkaEnvironment class, typically used for running integration tests. 

Based on the [Confluent Open Source distribution](https://www.confluent.io/product/confluent-open-source/) v7.2.x.

Instead of using the classic ports (2181, 9092, ...) for each server, the class will get the required number of available ports 
and use those in configurations for each server. 

```kotlin
class KafkaEnvironment(
    noOfBrokers: Int = 1,
    topicNames: List<String> = emptyList(),
    topicInfos: List<TopicInfo> = emptyList(),
    withSchemaRegistry: Boolean = false,
    val withSecurity: Boolean = false,
    users: List<JAASCredential> = emptyList(),
    autoStart: Boolean = false,
    brokerConfigOverrides: Properties = Properties()
) : AutoCloseable {
    data class TopicInfo(val name: String, val partitions: Int = 2, val config: Map<String, String>? = null)
}
 
fun start() // start servers in correct order
 
fun stop() // stop servers in correct order - session data are available
 
fun tearDown() // when finished with the kafka environment, stops servers and remove session data                    
```
Maximum allowed number of brokers is 2.

## Security
The 'withSecurity' parameter gives a kafka cluster with security, thus authentication and authorization
* Secured Zookeeper, kafka broker must authenticate and be authorized
* Secured Kafka broker (and inter broker), clients (including Schema registry) must authenticate and be authorized 
* No security for Schema Registry clients
* No TLS

See ```JAASContext.kt``` for details and predefined set of producer and consumer credentials.
Observe that auto creation of topics is disabled when security is enabled. 

The 'users' parameter is an option for custom set of producer and consumer credentials. Only relevant when 
security is enabled. 

## Getting Started
Add the dependency:

#### Gradle
```
dependencies {
    testImplementation "no.nav:kafka-embedded-env:3.2.2"
}
```

#### Maven
```
<dependency>
    <groupId>no.nav</groupId>
    <artifactId>kafka-embedded-env</artifactId>
    <version>3.2.2</version>
    <scope>test</scope>
</dependency>
```

**Note**: It is recommended that you use the Confluent version matching this library - currently v7.2.x

## Examples
### Default
```kotlin
val kafkaEnv = KafkaEnvironment()
 
kafkaEnv.start()
// do stuff
kafkaEnv.tearDown()
```

The default settings gives
* 1 Zookeeper
* 1 Kafka broker

### Custom 1
```kotlin
val kafkaEnv = KafkaEnvironment(
    noOfBrokers = 2,
    topicNames = listOf("test1", "test2", "test3"),
    withSchemaRegistry = true,
    autoStart = true
)
// do stuff
kafkaEnv.tearDown()
```
The above configuration gives 
* 1 Zookeeper instance
* 2 Kafka brokers
* 1 Schema Registry instance

Given topics are automatically created and all servers are started in correct order - ready to use.
Each topic will have number of partitions equal to number of brokers.

### Custom 2
```kotlin
val kafkaEnv = KafkaEnvironment(
    noOfBrokers = 2,
    topicNames = listOf("custom1"),
    withSecurity = true,
    users = listOf(JAASCredential("myP1", "myP1p"),JAASCredential("myC1", "myC1p")),
    autoStart = true
)
// do stuff
kafkaEnv.tearDown()
```
The above configuration gives 
* 1 Zookeeper instance
* 2 Kafka brokers

Given users are added to Kafka brokers JAAS context (authentication) and the topic is automatically created.
Observe that relevant authorization must be given before produce and consume scenario is activated.

See [Confluent authorization](https://docs.confluent.io/current/kafka/authorization.html). 

For 'crash course' approach, see relevant test cases with security in ```KafkaEnvironmentSpec.kt``` 

## ServerPark
An instance of KafkaEnvironment has a serverPark (ServerPark) property, giving access to details depending
on the state. Each server (ServerBase) has a few relevant properties and start/stop functions. 

```kotlin
data class ServerPark(
    val zookeeper: ServerBase,
    val brokerStatus: BrokerStatus,
    val schemaRegStatus: SchemaRegistryStatus,
    val status: ServerParkStatus
)
        
abstract class ServerBase {
    protected var status: ServerStatus = NotRunning

    open val host: String = "localhost"
    abstract val port: Int
    abstract val url: String

    abstract fun start()
    abstract fun stop()
}
``` 
Thus each server can be stopped and started independently.

In order to ease the state handling, some properties are available.

```kotlin
val zookeeper get() = serverPark.zookeeper as ZKServer
val brokers get() = serverPark.getBrokers()
val brokersURL get() = serverPark.getBrokersURL()
val adminClient get() = serverPark.getAdminClient()
val schemaRegistry get() = serverPark.getSchemaReg()
```
Be aware of what you are doing
* kafka environment without brokers (noOfBrokers = 0), gives empty list for 'brokers'
* non started kafka environment gives 'null' for 'adminClient' 
* a started kafka environment without brokers still gives 'null' for 'adminClient' 
* ...

The 'adminClient' property creates an instance of AdminClient with super user authorization. Thus, feel free to play
with it. See [Kafka AdminClient API](https://kafka.apache.org/20/javadoc/org/apache/kafka/clients/admin/AdminClient.html)
for the set of available operations. 

**Please close adminClient after use.**

## Changelog

### [3.2.2]
- Upgrade to Confluent 7.3.1
- Upgrade to Kafka 3.2.3
- Upgrade to Surefire 3.0.0-M8

### [3.2.1]
- Moved some dependencies from scope test

### [3.2.0]
- Fix Snyk issues

### [3.1.7]
- Upgrade to Confluent 7.2.1
- Upgrade to Ktor 2.0.3
- Upgrade to Kafka 3.2

### [3.1.4]
- Upgrade to Ktor 2.0.1
- Change to use Apache maven wrapper

### [3.1.3]
- Dependency bumps Confluent 7.1.1
- Github actions setup-java to v3

### [3.1.2]
- Bad merge fix

### [3.1.1]
- Upgrade to Kotlin 1.6.20
- Upgrade to Ktor 2.0.0
- Upgrade to Java to 17

### [3.1.0]
- Kafka 3.1.0
- Confluent Platform 7.1.0
- Kotlin 1.6.20
- Ktor 1.6.8
- Logback 1.2.11
- Slf4j 1.7.33

### [2.8.1]
- Multiple dependency bumps:
- Kafka 2.8.1
- Confluent Platform 6.2.1
- Kotlin 1.6.0
- Ktor 1.6.5
- Spek 2.0.17
- Slf4j 1.7.32
- Logback 1.2.7
- Kluent 1.68

### [2.8.0]
- Upgrade to Kafka 2.8.0
- Upgrade to Confluent Platform 6.2.0

### [2.7.0]
- Upgrade to Kafka 2.7.0
- Upgrade to Confluent Platform 6.1.0
- Changed to Kafka Scala build 2.13

### [2.5.0]
- Upgrade to Kafka 2.5.0
- Upgrade to Confluent Platform 5.5.0
- Fixes issue where config overrides were not passed to the AdminClient

### [2.4.0]
- Upgrade to Kafka 2.4.0
- Upgrade to Confluent Platform 5.4.0

### [2.3.0]
- Use Kafka 2.3.1
- Use Confluent 5.3.1

### [2.2.3]
- Increase default Zookeeper connection timeout from Kafka brokers in another attempt to remediate sporadically failing tests on slow machines

### [2.2.2]
- Another attempt at increasing waiting attempts for embedded Zookeeper in attempt to remediate sporadically failing tests on slow machines  

### [2.2.1]
- Increase timeouts against embedded Zookeeper in attempt to remediate sporadically failing tests on slow machines  

### [2.2.0]
- Upgrade to Kafka 2.3.0
- Upgrade to confluent platform 5.3.0

### [2.1.1]

- Improved path handling for temporary files (#7)

### [2.1.0]

#### What's new?

The possibility to override topic as well as broker configurations:

```
@param topicInfos a list of topics to create at environment startup - default empty
@param topicNames same as topicInfos, but for topics that can use the default values
@param brokerConfigOverrides possibility to override broker configuration
```

```kotlin
class KafkaEnvironment(
    noOfBrokers: Int = 1,
    topicNames: List<String> = emptyList(),
    topicInfos: List<TopicInfo> = emptyList(),
    withSchemaRegistry: Boolean = false,
    val withSecurity: Boolean = false,
    users: List<JAASCredential> = emptyList(),
    autoStart: Boolean = false,
    brokerConfigOverrides: Properties = Properties()
) : AutoCloseable {
    data class TopicInfo(val name: String, val partitions: Int = 2, val config: Map<String, String>? = null)
}
```

#### Breaking changes from 2.0.x

`class KafkaEnvironment`:
 - The parameter `topics` should be renamed to `topicNames` if you wish to keep the behaviour from previous versions.

## Contact

Create an issue here on the GitHub issue tracker. Pull requests are also welcome.

Internal resources may reach us on Slack in the #kafka channel.
