# Spring Batch MongoDB Support

[Spring Batch](http://static.springsource.org/spring-batch) is a framework for Java enterprise batch processing.

This project offers support for accessing a [MongoDB NoSQL datastore](http://www.mongodb.org) from your Spring Batch batch jobs. It provides a

* [MongoDBItemReader](https://github.com/ttrelle/spring-batch-mongodb-support/blob/master/src/main/java/org/springframework/batch/item/mongodb/MongoDBItemReader.java)
* [MongoDBItemWriter](https://github.com/ttrelle/spring-batch-mongodb-support/blob/master/src/main/java/org/springframework/batch/item/mongodb/MongoDBItemWriter.java)

# Usage

After cloning the project run

	mvn clean javadoc:javadoc install
	
from the command line. To execute the unit tests you need a running MongoDB server on your local machine listening on the
standard port 27017. You can override the location to your MongoDB server by passing the following arguments to Maven:

	mvn -DargLine="-Dhost=anotherHost -Dport=4711" test

There are examples how to use the reader and writer in your batch jobs:

* [MongoDBItemReaderIntegrationTest](https://github.com/ttrelle/spring-batch-mongodb-support/blob/master/src/test/java/org/springframework/batch/item/mongodb/example/MongoDBItemReaderIntegrationTest.java) and
  [MongoDBItemReaderIntegrationTest-context.xml](https://github.com/ttrelle/spring-batch-mongodb-support/blob/master/src/test/resources/org/springframework/batch/item/mongodb/example/MongoDBItemReaderIntegrationTest-context.xml)
* [MongoDBItemWriterIntegrationTest](https://github.com/ttrelle/spring-batch-mongodb-support/blob/master/src/test/java/org/springframework/batch/item/mongodb/example/MongoDBItemWriterIntegrationTest.java) and
  [MongoDBItemWriterIntegrationTest-context.xml](https://github.com/ttrelle/spring-batch-mongodb-support/blob/master/src/test/resources/org/springframework/batch/item/mongodb/example/MongoDBItemWriterIntegrationTest-context.xml)

# Dependencies

This project depends only on
* [Spring Batch](https://github.com/SpringSource/spring-batch)
* [MongoDB Java Driver](https://github.com/mongodb/mongo-java-driver)
