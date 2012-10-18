package org.springframework.batch.item.mongodb;

import com.mongodb.DBObject;

/**
 * This reader reads items from a MongoDB collection and returns
 * {@link DBObject}s from the Java driver API.
 * 
 * @author Tobias Trelle
 */
public class MongoDBRawItemReader extends AbstractMongoDBItemReader<DBObject> {

	/** A RuntimeException w/ this msg signals: no more documents can be read. */
	private static final String NO_MORE = "no more";
	
	@Override
	public DBObject doRead() throws Exception {
		try {
			return cursor.next();
		
		} catch (RuntimeException e) {
			if (NO_MORE.equals(e.getMessage())) {
				return null;
			} else {
				throw e;
			}
				
		}
	}
	

	
}
