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
 * MongoDB does not support transactions, but we have a {@link #transactional} flag here. What is it supposed to do?
 * Spring Batch has a very strong transactional model, which is important for features like restart, skip and retry.
 * We cannot use these features with MongoDB, because MongoDB is not transactional. If there's an error while 
 * writing, it may be that some items have been written to disk and some not.
 * Since we cannot change that behaviour, our goal is now to isolate the one case that makes it impossible to use
 * Spring Batch features like restart, skip and retry. We want one dedicated error situation that tells us that
 * something went wrong with a MongoDB insertion and we cannot be sure about transactional safety, so we better
 * check and definitely not use restart. In all other cases we are safe to use restart, skip and retry. This error 
 * situation is throwing a {@link MongoDBInsertFailedException}.
 * In short: when {@link #transactional} is set to {@code true} and there is no {@link MongoDBInsertFailedException} 
 * involved, you're safe to use restart, skip and retry. Of course, this makes more sense when combining different
 * resources in a job.
 * How does it work?
 * When {@link #transactional} is set to {@code true}, this writer builds up a cache of {@link com.mongodb.DBObject}s 
 * to write and delays writing them to MongoDB until the transaction has been successfully committed. We make use of Spring's 
 * transaction synchronization features here. Because exceptions in transaction synchronizations do not get propagated,
 * we catch a potential throwable and use a {@link org.springframework.batch.core.ChunkListener} to re-throw it.  
 * 
 * When {@link #transactional} is set to {@code true}, this writer is *not* thread-safe.
 * 
 * @author Tobias Trelle, Tobias Flohre
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
				public void afterCompletion(int status) {
					try {
						if (status == STATUS_COMMITTED) {
							doInsert(db, collection, wc, dbObjectCache);
						}
					} catch (Throwable t) {
						mongoDbFailure = t;
					} finally {
						dbObjectCache = null;
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
		try {
			if (mongoDbFailure != null){
				if (mongoDbFailure instanceof MongoDBInsertFailedException){
					throw (MongoDBInsertFailedException)mongoDbFailure;
				} else {
					throw new MongoDBInsertFailedException(db, collection, "Could not insert document/s into collection", mongoDbFailure);
				}
			}
		} finally {
			mongoDbFailure = null;
		}
	}

	/**
	 * Perform the insert operation on the collection.
	 * 
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
