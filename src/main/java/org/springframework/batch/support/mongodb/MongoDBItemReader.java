package org.springframework.batch.support.mongodb;

import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

/**
 * This reader reads items from a MongoDB collection.
 * @author Tobias Trelle
 *
 * @param <T> Type of mapped object.
 */
public class MongoDBItemReader<T> extends AbstractItemCountingItemStreamItemReader<T> {
	
	// configurable attributes ......................................
	
	/** MongoDB connection pool. */
	private Mongo mongo;
	
	/** Name of the database to read from. */
	private String db;
	
	/** Name of the collection to read from. */
	private String collection;
	
	/** 
	 * Query in JSON notation, e.g. <code>{a: 1, b: 2}</code> (optional).
	 * <p/>
	 * If no query is given, the whole collection is read.
	 */
	private String query;

	private DocumentMapper<T> mapper;
	
	// internally used attributes ......................................
	
	/** MongoDB database abstraction. */
	private DB mongoDB;
	
	/** Cursor pointing to the current document. */
	private DBCursor cursor;
	
	@Override
	protected void jumpToItem(int itemIndex) throws Exception {
		// TODO Auto-generated method stub
		cursor.skip(itemIndex);
	}

	@Override
	protected void doOpen() throws Exception {
				// TODO Auto-generated method stub
		mongoDB = mongo.getDB(db);
		DBCollection coll = mongoDB.getCollection(collection);
		
		// TODO add (otional) query
		cursor = coll.find();
	}

	@Override
	protected T doRead() throws Exception {
		DBObject document = cursor.next();
		
		return mapper.map(document);
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

	public DocumentMapper<T> getMapper() {
		return mapper;
	}

	public void setMapper(DocumentMapper<T> mapper) {
		this.mapper = mapper;
	}

	
	
}
