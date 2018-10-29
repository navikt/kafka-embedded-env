[![Build Status](https://travis-ci.org/navikt/kafka-embedded-env.svg?branch=master)](https://travis-ci.org/navikt/kafka-embedded-env) 
[![Published on Maven](https://img.shields.io/maven-metadata/v/http/central.maven.org/maven2/no/nav/kafka-embedded-env/maven-metadata.xml.svg)](http://central.maven.org/maven2/no/nav/kafka-embedded-env/)

# kafka-embedded-env 

A simple API for creating an embedded Kafka environment with the KafkaEnvironment class, typically used for running integration tests. 

Based on the [Confluent Open Source distribution](https://www.confluent.io/product/confluent-open-source/) v5.0.0. 

Instead of using the classic ports (2181, 9092, ...) for each server, the class will get the required number of available ports 
and use those in configurations for each server. 

```kotlin
class KafkaEnvironment(
    noOfBrokers: Int = 1,
    val topics: List<String> = emptyList(),
    withSchemaRegistry: Boolean = false,
    val withSecurity: Boolean = false,
    users: List<JAASCredential> = emptyList(),
    autoStart: Boolean = false
) : AutoCloseable
  
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
    testImplementation "no.nav:kafka-embedded-env:2.0.0"
}
```

#### Maven
```
<dependency>
    <groupId>no.nav</groupId>
    <artifactId>kafka-embedded-env</artifactId>
    <version>2.0.0</version>
    <scope>test</scope>
</dependency>
```

**Note**: It is recommended that you use the Confluent version matching this library - currently v5.x
(i.e. Kafka v2.x, though it is likely that Confluent v4.x/Kafka v1.x will also work)

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
    topics = listOf("test1", "test2", "test3"),
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
    topics = listOf("custom1"),
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


## Contact

Create an issue here on the GitHub issue tracker. Pull requests are also welcome.

Internal resources may reach us on Slack in the #kafka channel.