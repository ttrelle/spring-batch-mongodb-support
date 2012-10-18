package org.springframework.batch.item.mongodb;

import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.util.JSON;
import com.mongodb.util.JSONParseException;

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
	protected String keys;

	
	// internally used attributes ......................................
	
	/** MongoDB database abstraction. */
	protected DB mongoDB;
	
	/** Cursor pointing to the current document. */
	protected DBCursor cursor;
	
	public abstract T doRead() throws Exception;
	
	@Override
	protected void jumpToItem(int itemIndex) throws Exception {

		if (itemIndex < 0) {
			throw new IllegalArgumentException("index must not be negative");
		}
		
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
		DBObject keysDoc = null;
		
		if ( query != null ) {
			ref = parseDocument(query);
		}
		
		if ( keys != null ) {
			keysDoc = parseDocument(keys);
		}
		
		cursor = coll.find(ref, keysDoc);
	}
	
	private DBObject parseDocument(String json) {
		try {
			return (DBObject)JSON.parse(json);
		} catch (JSONParseException e) {
			throw new IllegalDocumentException("Not a valid document: " + json, e);
		}
	}
	
	@Override
	protected void doClose() throws Exception {
		if ( cursor != null ) {
			cursor.close();
		}
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

	public void setCollection(String collection) {
		this.collection = collection;
	}


	public void setQuery(String query) {
		this.query = query;
	}
	
	public void setKeys(String keys) {
		this.keys = keys;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(mongo, "A Mongo instance is required");
		Assert.notNull( db, "A database name is required" );
		Assert.notNull( collection, "A collection name is required" );
	}
	
}
