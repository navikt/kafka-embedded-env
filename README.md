[![Build Status](https://travis-ci.org/navikt/kafka-embedded-env.svg?branch=master)](https://travis-ci.org/navikt/kafka-embedded-env) 
[![Published on Maven](https://img.shields.io/maven-metadata/v/http/central.maven.org/maven2/no/nav/kafka-embedded-env/maven-metadata.xml.svg)](http://central.maven.org/maven2/no/nav/kafka-embedded-env/)

# kafka-embedded-env 

A simple API for creating an embedded Kafka environment with the KafkaEnvironment class, typically used for running integration tests. 

Based on the [Confluent Open Source distribution](https://www.confluent.io/product/confluent-open-source/) v5.0.0. 

Instead of using the classic ports (2181, 9092, ...) for each server, the class will get the required number of available ports 
and use those in configurations for each server. 

```kotlin
class KafkaEnvironment(
    val noOfBrokers: Int = 1,
    val topics: List<String> = emptyList(),
    withSchemaRegistry: Boolean = false,
    withKSQL: Boolean = false,
    // withRest: Boolean = false, //see note below
    autoStart: Boolean = false
)
  
fun start() // start servers in correct order
 
fun stop() // stop servers in correct order - session data are available
 
fun tearDown() // when finished with the kafka environment, stops servers and remove session data                    
```
**Note**: Due to dependency between REST server v5 and kafka 1.1.1 (libraries conflict), the REST server is disabled 
for the time being.

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

### Custom
```kotlin
val kafkaEnv = KafkaEnvironment(
    noOfBrokers = 2,
    topics = listOf("test1", "test2", "test3"),
    withKSQL = true,
    autoStart = true
)
// do stuff
kafkaEnv.tearDown()
```
The above custom configuration gives 
* 1 Zookeeper instance
* 2 Kafka brokers
* 1 Schema Registry instance (automatically started if KSQL or Kafka REST is enabled)
* 1 KSQL instance

Given topics are automatically created and all servers are started in correct order - ready to use.
Each topic will have number of partitions equal to number of brokers.

## ServerPark
An instance of KafkaEnvironment has a serverPark (ServerPark) property, giving access to configured servers.
Each server (ServerBase) has a few relevant properties and start/stop functions. 

```kotlin
data class ServerPark(
    val zookeeper: ServerBase,
    val brokers: List<ServerBase>,
    val schemaregistry: ServerBase,
    val ksql: ServerBase,
    val rest: ServerBase
)
        
abstract class ServerBase {
    protected var status: ServerStatus = NotRunning

    open val host: String = "localhost"
    abstract val port: Int
    abstract val url: String

    abstract fun start()
    abstract fun stop()
}

val brokersURL: String
val adminClient: AdminClient
``` 
Thus each server can be stopped and started independently.

The `brokersURL` property provides a comma-separated string of the running brokers' addresses.

The 'adminClient' property provides an instance of AdminClient. 
See [Kafka AdminClient API](https://kafka.apache.org/20/javadoc/org/apache/kafka/clients/admin/AdminClient.html)
for the set of available operations. 


## Contact

Create an issue here on the GitHub issue tracker. Pull requests are also welcome.

Internal resources may reach us on Slack in the #kafka channel.