package org.springframework.batch.item.mongodb;

import java.util.List;

import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.util.JSON;
import com.mongodb.util.JSONParseException;

/**
 * This item reader reads documents from a MongoDB collection.
 * <p/>
 * Required parameters are:
 * <ul>
 * <li>{@link #mongo}: a reference to a {@link Mongo} connection pool.</li>
 * <li>{@link #db}: Name of the database to use. 
 * 		If the database does not exist on the MongoDB server an error occurs.</li>
 * <li>{@link #collection}: Name of the collection to read from. By default, all
 * 		documents from that collection are read.
 * 		If the collection does not exist inside the database an error occurs.</li>
 * </ul>
 * <p/>
 * Optional parameters are:
 * <ul>
 * <li>{@link #query}: a query string in JSON notation to restrict the set of read documents,
 * 		e.g. <code>{a:1, $or: {b: "foo"}}</code></li>
 * <li>{@link #keys}: the set of attributes to read from each single document. By default,
 * 		all key/value pairs are read. If you specify a key set, only the data from these
 * 		keys are read, e.g. {_id:0, a:1} return only the data for key <code>a</code></li>
 * <li>{@link #converter}: By default, this reader returns instances of {@link DBObject} from
 * 		the API of the MongoDB Java driver. If you want to use another format in your
 * 		item processor, provide a converter that implements the {@link DocumentObjectConverter }
 * 		interface.</li>
 * <li>{@link #sort}: Optional sort criteria.</li>
 * <li>{@link #limit}: Optional limit for amounts of read documents.</li>
 * <li>{@link #batchSize}: Size of batch reads.</li>
 * <li>{@link #snapshot}: Use a query snapshot or not.</li>
 * </ul>
 * 
 * @author Tobias Trelle
 */
public class MongoDBItemReader 
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
	 * JSON document that filters the returned fields (optinal).
	 */
	protected String keys;
	
	/** Custom converter to map {@DBObject}s to Java POJOs (optional). */
	protected DocumentObjectConverter<?> converter;

	/** 
	 * Number of documents to read in one batch (optional).
	 * @see DBCursor#batchSize(int) 
	 */ 
	protected int batchSize;
	
	/** 
	 * Sort criteria in JSON notation,e.g. <code>{a: -1, b: 1}</code> (optional).
	 * @see DBCursor#sort(DBObject) 
	 */
	protected String sort;

	/**
	 * Use a snapshot query (optional). Default is <code>false</code>.
	 * @see DBCursor#snapshot()
	 */
	protected boolean snapshot;
	
	/**
	 * Limit the amount of read documents (optional).
	 * @see DBCursor#limit(int)
	 */
	protected int limit;
	
	
	// internally used attributes ......................................
	
	/** Cursor pointing to the current document. */
	protected DBCursor cursor;

	
	// public item reader interface .........................................
	
	public MongoDBItemReader() {
		setName(ClassUtils.getShortName(MongoDBItemReader.class));
	}
	
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
		
		final DB mongoDB = mongo.getDB(db);
		
		// do NOT read from collections that do not exist
		if ( !mongoDB.collectionExists(collection) ) {
			throw new IllegalArgumentException("No such collection: " + collection);
		}

		// create the cursor
		cursor = createCursor(mongoDB.getCollection(collection));
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
	
	private DBCursor createCursor(DBCollection coll) {
		DBCursor crsr;
		DBObject ref = null;
		DBObject keysDoc = null;
		
		if ( StringUtils.hasText(query) ) {
			ref = parseDocument(query);
		}
		
		if ( StringUtils.hasText(keys) ) {
			keysDoc = parseDocument(keys);
		}
		
		crsr = coll.find(ref, keysDoc);	
		
		if ( StringUtils.hasText(sort) ) {
			crsr = crsr.sort(parseDocument(sort));
		}
		
		if ( batchSize != 0 ) {
			crsr = crsr.batchSize(batchSize);
		}
		
		if ( snapshot ) {
			crsr = crsr.snapshot();
		}

		if ( limit != 0) {
			crsr = crsr.limit(limit);
		}
		
		return crsr;
	}
	
	private static DBObject parseDocument(String json) {
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
	
	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}

	public void setSort(String sort) {
		this.sort = sort;
	}
	
	public void setSnapshot(boolean snapshot) {
		this.snapshot = snapshot;
	}

	public void setLimit(int limit) {
		this.limit = limit;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(mongo, "A Mongo instance is required");
		Assert.hasText( db, "A database name is required" );
		Assert.hasText( collection, "A collection name is required" );
	}
	
}
