###  Distributed transaction processing using event-driven kafka saga choreography and Most useful kafka configuration details.
KAFKA_LOG_DIRS: KAFKA_LOG_DIRS defines where Kafka stores its persistent data.Kafka persists
all messages to disk, and these log segments are stored in the directory specified in the log.dir config.
Messages for each topic and partition
Log segments
Offsets
Snapshots
Metadata (in KRaft mode)



### Every broker must be configured with the following core KRaft properties:

#### Mandatory broker configuration in KRaft
node.id=1
Unique integer per Kafka process
Used by controllers to identify the broker.

process.roles=broker
OR
process.roles=broker,controller

broker → handles data
controller → participates in metadata quorum

## Controller quorum configuration
controller.quorum.voters=1@host1:9093,2@host2:9093,3@host3:9093
All brokers must know how to reach controllers
Required even if broker itself is not a controller

## Controller listener
controller.listener.names=CONTROLLER
And in listeners:
listeners=PLAINTEXT://host:9092,CONTROLLER://host:9093
Controllers communicate via a dedicated listener
Brokers never use controller listeners for client traffic

## Cluster ID (CRITICAL)
cluster.id= MkU3OEVBNTcwNTJENDM2Qk
All nodes must use the same cluster ID
Stored on disk after formatting
Ensures the broker joins the correct cluster

## Log directory
log.dirs=/var/lib/kafka/data
Stores:
Partition data
Metadata snapshots
Raft log state

## Storage formatting (REQUIRED ONCE)
Before a broker can join a KRaft cluster, you must format its storage
Writes meta.properties into log.dirs
Stores:
cluster.id
node.id
Prevents brokers from accidentally joining the wrong cluster
Note: This step is only mandatory for new nodes.

## Adding a new broker to an existing KRaft cluster
Choose a new unique node.id
Use the same cluster.id
Use the same controller.quorum.voters
Format storage
Start Kafka

node.id=4
process.roles=broker
cluster.id= MkU3OEVBNTcwNTJENDM2Qk

Kafka automatically:
Registers broker
Makes it eligible for partition assignments
Rebalances leaders (if triggered)

## Kafka broker configurations
| Component         | Role                                          |
| ----------------- | --------------------------------------------- |
| Controller(s) | Manage metadata (topics, partitions, brokers) |
| Broker(s)     | Store data, serve producers & consumers       |
| Metadata log  | Raft-based log storing cluster state          |
| Cluster ID    | Unique ID identifying the Kafka cluster       |

#

#Kafka consumer configurations

| Property         | Purpose                          |
|------------------| -------------------------------- |
| bootstrap.servers | Initial Kafka brokers to connect |
| group.id         | Identifies the consumer group    |
| key.deserializer | Converts key bytes → object      |
| value.deserializer | Converts value bytes → object    |