package org.springframework.batch.item.mongodb;

/**
 * @author tobias.flohre
 *
 */
public class MongoDBInsertFailedException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public MongoDBInsertFailedException() {
		super();
	}

	public MongoDBInsertFailedException(String message, Throwable cause) {
		super(message, cause);
	}

	public MongoDBInsertFailedException(String message) {
		super(message);
	}

	public MongoDBInsertFailedException(Throwable cause) {
		super(cause);
	}


}
