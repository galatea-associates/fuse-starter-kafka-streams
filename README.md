# kafka-starter
Starter project for Galatea Kafka java projects

### Using Transformers
* TaskStore key type must be a superset of the partition key 
* each distinct partition key may be in a separate store from all other partition keys
* each distinct partition key may be in the same store as all other partition keys