package org.springframework.batch.item.mongodb;

import java.util.ArrayList;
import java.util.List;

import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.transaction.support.TransactionSynchronizationAdapter;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.Assert;

import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;

/**
 * This item writer writes documents to a MongoDB collection.
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
 * <li>{@link #converter}</li>
 * <li>{@link #writeConcern}</li>
 * <li>{@link #transactional}</li>
 * </ul>
 * 
 * @author Tobias Trelle
 */
public class MongoDBItemWriter implements ItemWriter<Object>, InitializingBean, ChunkListener {
	
	/** By default, a writer is transaction aware. */
	private static final boolean DEFAULT_TRANSACTIONAL = true;
	
	/** 
	 * By default, we check the write result after a chunk of documents is inserted.
	 * We may get a better performance if you set this flag to <code>false</code>.
	 */
	private static final boolean DEFAULT_CHECK_WRITE_RESULT = true;
	
	// configurable attributes ......................................
	
	/** MongoDB connection pool. */
	protected Mongo mongo;
	
	/** Name of the database to read from. */
	protected String db;
	
	/** Name of the collection to read from. */
	protected String collection;
	
	/** Custom converter (optional). */
	protected ObjectDocumentConverter converter;
	
	/** Overwrite the write concern of the target collection (optional). */
	protected WriteConcern writeConcern;
	
	/**
	 * Flag to indicate that writing to MongoDB should be delayed if a
	 * transaction is active. Defaults to true.
	 */
	protected boolean transactional = DEFAULT_TRANSACTIONAL;
	
	/**
	 * Flag to indicate if the write result should be checked for error
	 * after writing a chunk of documents.
	 */
	protected boolean checkWriteResult = DEFAULT_CHECK_WRITE_RESULT;
	
	private List<DBObject> dbObjectCache = null;
	
	private Throwable mongoDbFailure = null;

	// public item writer interface .........................................
	
	@Override
	public void write(List<? extends Object> items) throws Exception {
		final WriteConcern wc = writeConcern == null ? mongo.getWriteConcern() : writeConcern;
		
		if (transactional && TransactionSynchronizationManager.isActualTransactionActive()) {
			dbObjectCache = prepareDocuments(items);
			TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronizationAdapter() {

				@Override
				public void afterCommit() {
					try {
						doInsert(db, collection, wc, dbObjectCache);
					}
					catch (Throwable t) {
						mongoDbFailure = t;
					}
				}

			});
		} else {
			doInsert(db, collection, wc, prepareDocuments(items) );
		}
	}
	
	// private methods .....................................................
	private List<DBObject> prepareDocuments(List<? extends Object> items)  {
		final List<DBObject> docs = new ArrayList<DBObject>();
		
		if ( items != null ) {
			for ( Object item: items ) {
				if ( item instanceof DBObject ) {
					docs.add( (DBObject)item);
				} else if (converter != null) {
					docs.add( converter.convert(item) );
				} else {
					throw new IllegalArgumentException("Cannot convert item to DBObject: " + item);
				}
			}
		}
		
		return docs;
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
	
	public void setConverter(ObjectDocumentConverter converter) {
		this.converter = converter;
	}

	public void setWriteConcern(WriteConcern writeConcern) {
		this.writeConcern = writeConcern;
	}
	
	/**
	 * Flag to indicate that writing to MongoDB should be delayed if a
	 * transaction is active. Defaults to true.
	 */
	public void setTransactional(boolean transactional) {
		this.transactional = transactional;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(mongo, "A Mongo instance is required");
		Assert.hasText( db, "A database name is required" );
		Assert.hasText( collection, "A collection name is required" );
	}

	@Override
	public void beforeChunk() {
		// Nothing to do.
	}

	@Override
	public void afterChunk() {
		dbObjectCache = null;
		if (mongoDbFailure != null){
			if (mongoDbFailure instanceof MongoDBInsertFailedException){
				throw (MongoDBInsertFailedException)mongoDbFailure;
			} else {
				throw new MongoDBInsertFailedException(db, collection, "Could not insert document/s into collection", mongoDbFailure);
			}
		}
	}

	/**
	 * Perform the insert operation on the collection.
	 * 
	 * @param m Mongo connection pool,
	 * @param databaseName Name of the database to use.
	 * @param collectionName Name of the collection to use.
	 * @param wc WriteConcern.
	 * @param docs List of documents to insert.
	 */
	 void doInsert(String databaseName, String collectionName, WriteConcern wc, List<DBObject> docs) {
		WriteResult wr = mongo.getDB(databaseName).getCollection(collectionName).insert(docs, wc);
		
		// strange: if no documents are written (list null or empty) the write result is also null
		if ( checkWriteResult &&  wr != null && wr.getError() != null ) {
			throw new MongoDBInsertFailedException(databaseName, collectionName, wr.getError() );
		}
	}
	
}
