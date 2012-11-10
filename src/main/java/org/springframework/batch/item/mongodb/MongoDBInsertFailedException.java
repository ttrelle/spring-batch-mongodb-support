package org.springframework.batch.item.mongodb;

/**
 * This exception is thrown when an insert operation on a collection fails.
 * 
 * @author Tobias Flohre
 */
public class MongoDBInsertFailedException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public MongoDBInsertFailedException() {
		super();
	}

	public MongoDBInsertFailedException(String db, String collection, String message, Throwable cause) {
		super(createMessage(db, collection, message), cause);
	}

	public MongoDBInsertFailedException(String message, Throwable cause) {
		super(message, cause);
	}
 
	public MongoDBInsertFailedException(String db, String collection, String message) {
		super(createMessage(db, collection, message));
	}

	private static String createMessage(String db, String collection, String message) {
		return "db=" + db + ", collection=" + collection + ": " + message;
	}

}
