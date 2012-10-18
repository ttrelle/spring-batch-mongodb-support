package org.springframework.batch.item.mongodb;

import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.DataAccessException;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.util.JSON;

/**
 * This reader reads items from a MongoDB collection.
 * @author Tobias Trelle
 *
 * @param <T> Type of mapped object.
 */
abstract class AbstractMongoDBItemReader<T> extends AbstractItemCountingItemStreamItemReader<T> implements InitializingBean {
	
	// configurable attributes ......................................
	
	/** MongoDB connection pool. */
	protected Mongo mongo;
	
	/** Name of the database to read from. */
	protected String db;
	
	/** Name of the collection to read from. */
	protected String collection;
	
	/** 
	 * Query in JSON notation, e.g. <code>{a: 1, b: 2}</code> (optional).
	 * <p/>
	 * If no query is given, the whole collection is read.
	 */
	protected String query;
	
	/**
	 * JSON document that filters the returned fields.
	 */
	protected String fields;

	
	// internally used attributes ......................................
	
	/** MongoDB database abstraction. */
	protected DB mongoDB;
	
	/** Cursor pointing to the current document. */
	protected DBCursor cursor;
	
	@Override
	protected void jumpToItem(int itemIndex) throws Exception {
		// TODO Auto-generated method stub
		cursor.skip(itemIndex);
	}

	@Override
	protected void doOpen() throws Exception {
		mongoDB = mongo.getDB(db);
		
		// do NOT read from collections that do not exist
		if ( !mongoDB.collectionExists(collection) ) {
			throw new CollectionDoesNotExistsException("No such collection: " + collection);
		}
		
		DBCollection coll = mongoDB.getCollection(collection);
		
		DBObject ref = null;
		DBObject keys = null;
		
		if ( query != null ) {
			ref = (DBObject)JSON.parse(query);
		}
		if ( fields != null ) {
			keys = (DBObject)JSON.parse(fields);
		}
		cursor = coll.find(ref, keys);
	}
	
	@Override
	protected void doClose() throws Exception {
		if ( cursor != null ) {
			cursor.close();
		}
	}

	public Mongo getMongo() {
		return mongo;
	}

	public void setMongo(Mongo mongo) {
		this.mongo = mongo;
	}

	public String getDb() {
		return db;
	}

	public void setDb(String db) {
		this.db = db;
	}

	public String getCollection() {
		return collection;
	}

	public void setCollection(String collection) {
		this.collection = collection;
	}

	public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		this.query = query;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(mongo, "An Mongo instance is required");
		// TODO check more required parameters
		
	}
	
}
