package org.springframework.batch.item.mongodb;

import java.util.List;

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
public class MongoDBCursorItemReader 
	extends AbstractItemCountingItemStreamItemReader<Object> 
	implements InitializingBean {
	
	/** A RuntimeException w/ this msg signals: no more documents can be read. */
	private static final String NO_MORE = "no more";
	
	
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
	
	/** Optional custom converter. */
	protected DocumentObjectConverter<?> converter;

	
	// internally used attributes ......................................
	
	/** MongoDB database abstraction. */
	protected DB mongoDB;
	
	/** Cursor pointing to the current document. */
	protected DBCursor cursor;

	
	// public item reader interface .........................................
	
	@Override
	protected void jumpToItem(int itemIndex) throws Exception {

		if (itemIndex < 0) {
			throw new IllegalArgumentException("Index must not be negative: " + itemIndex);
		}
		
		cursor.skip(itemIndex);
	}

	@Override
	protected void doOpen() throws Exception {
		
		// do NOT read from a db that does not exist
		if ( !dbExists() ) {
			throw new IllegalArgumentException("No such database: " + db);
		}
		
		mongoDB = mongo.getDB(db);
		
		// do NOT read from collections that do not exist
		if ( !mongoDB.collectionExists(collection) ) {
			throw new IllegalArgumentException("No such collection: " + collection);
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
	
	@Override
	public Object doRead() throws Exception {
		try {
			
			return converter != null ? converter.convert(cursor.next() ) : cursor.next();
		
		} catch (RuntimeException e) {
			if (NO_MORE.equals(e.getMessage())) {
				return null;
			} else {
				throw e;
			}
				
		}
	}	
	
	@Override
	protected void doClose() throws Exception {
		if ( cursor != null ) {
			cursor.close();
		}
	}	

	
	// Internal methods .....................................................
	
	private boolean dbExists() {
		List<String> dbNames = mongo.getDatabaseNames();
		
		return dbNames != null && dbNames.contains(db);
	}
	
	private DBObject parseDocument(String json) {
		try {
			return (DBObject)JSON.parse(json);
		} catch (JSONParseException e) {
			throw new IllegalArgumentException("Not a valid JSON document: " + json, e);
		}
	}

	
	// Setter ...............................................................
	
	public void setMongo(Mongo mongo) {
		this.mongo = mongo;
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
	
	public void setConverter(DocumentObjectConverter<?> converter) {
		this.converter = converter;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(mongo, "A Mongo instance is required");
		Assert.notNull( db, "A database name is required" );
		Assert.notNull( collection, "A collection name is required" );
	}
	
}
