# kafka.embedded

A simple API for creating an embedded kafka environment with the KafkaEnvironment class. 
Based on confluent.io version 4 environment. 

Instead of using the classic ports (2181,9092...) for each server, the class will get the required number of available port 
and use those in configurations for each server. 

```kotlin
class KafkaEnvironment(val noOfBrokers: Int = 1,
                       val topics: List<String> = emptyList(),
                       withSchemaRegistry: Boolean = false,
                       withRest: Boolean = false,
                       autoStart: Boolean = false)...
  
fun start() // start servers in correct order
 
fun stop() // stop servers in correct order - session data are available
 
fun tearDown() // when finished with the kafka environment, stops servers and remove session data                    
```

## Examples
### Default
```kotlin
val keDefault = KafkaEnvironment()
 
keDefault.start()
  
// whatever use scenario
 
keDefault.tearDown()
```

The default settings gives
* 1 zookeeper
* 1 broker

### Enhanced
```kotlin
val keEnhanced = KafkaEnvironment(
                    noOfBrokers = 3,
                    topics = listOf("test1","test2","test3"),
                    withRest = true,
                    autoStart = true)
  
// whatever use scenario
 
keEnhanced.tearDown()
```
Enhanced configuration gives 
* 1 zookeeper
* 3 brokers
* 1 schema registry (due to use of kafka rest)
* 1 kafka rest

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
            val rest: ServerBase)
    ...        
    abstract class ServerBase {
        protected var status: ServerStatus = NotRunning
    
        open val host: String = "localhost"
        abstract val port: Int
        abstract val url: String
    
        abstract fun start()
        abstract fun stop()
    }
    ...
    val brokersURL: String
``` 
Thus, each server can be stopped and started independent of the kafka environment start/stop.
In addition, a brokersURL property with all brokers, is available. 