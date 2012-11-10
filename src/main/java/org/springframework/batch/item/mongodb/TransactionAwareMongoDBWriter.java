package org.springframework.batch.item.mongodb;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.springframework.core.NamedThreadLocal;
import org.springframework.transaction.support.TransactionSynchronizationAdapter;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.WriteConcern;

/**
 * Delays actually writing to a MongoDB if a transaction is active.
 * 
 * @author Tobias Flohre
 */
public class TransactionAwareMongoDBWriter {
	
	private static final String MONGO_KEY_PREFIX = TransactionAwareMongoDBWriter.class.getName() + ".MONGO_KEY";

	private Mongo mongo;
	private String mongoKey;
	private static final ThreadLocal<Throwable> mongoDbFailure =
			new NamedThreadLocal<Throwable>("Mongo DB failure on insert");
	
	public TransactionAwareMongoDBWriter(Mongo mongo){
		this.mongo = mongo;
		this.mongoKey = MONGO_KEY_PREFIX + hashCode();
	}
	
	public void writeToCollection(String db, String collection, WriteConcern writeConcern, List<DBObject> objectsToWrite){
		if (transactionActive()){
			Map<MongoDBCollectionKey, List<DBObject>> objectsToWriteMap = getObjectsToWriteMap();
			MongoDBCollectionKey dbCollectionKey = new MongoDBCollectionKey(db, collection, writeConcern);
			List<DBObject> objectsToWriteList = objectsToWriteMap.get(dbCollectionKey);
			if (objectsToWriteList == null){
				objectsToWriteMap.put(dbCollectionKey, objectsToWrite);
			} else {
				objectsToWriteList.addAll(objectsToWrite);
			}
		} else {
			DBCollection coll = mongo.getDB(db).getCollection(collection);
			WriteConcern wc = writeConcern == null ? coll.getWriteConcern() : writeConcern;
			coll.insert(objectsToWrite, wc);
		}
	}
	
	public Throwable getMongoDBFailure(){
		return mongoDbFailure.get();
	}
	
	public void resetMongoDBFailure() {
		mongoDbFailure.remove();
	}

	@SuppressWarnings("unchecked")
	private Map<MongoDBCollectionKey, List<DBObject>> getObjectsToWriteMap(){
		if (!TransactionSynchronizationManager.hasResource(mongoKey)) {
			TransactionSynchronizationManager.bindResource(mongoKey, new HashMap<MongoDBCollectionKey, List<DBObject>>());
			TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronizationAdapter() {

				@Override
				public void afterCompletion(int status) {
					try {
						if (status == STATUS_COMMITTED) {
							complete();
						}
					}
					catch (Throwable t) {
						mongoDbFailure.set(t);
						throw new MongoDBInsertFailedException("Could not write to Mongo DB", t);
					}
					finally {
						clear();
					}
				}

				private void clear() {
					if (TransactionSynchronizationManager.hasResource(mongoKey)) {
						TransactionSynchronizationManager.unbindResource(mongoKey);
					}
				}

				private void complete() {
					Map<MongoDBCollectionKey, List<DBObject>> objectsToWriteMap = (Map<MongoDBCollectionKey, List<DBObject>>) TransactionSynchronizationManager.getResource(mongoKey);
					for (Entry<MongoDBCollectionKey, List<DBObject>> entry: objectsToWriteMap.entrySet()){
						DBCollection coll = mongo.getDB(entry.getKey().getDb()).getCollection(entry.getKey().getCollection());
						WriteConcern wc = entry.getKey().getWriteConcern() == null ? coll.getWriteConcern() : entry.getKey().getWriteConcern();
						coll.insert(entry.getValue(), wc);
					}
				}
				
			});
		}
		return (Map<MongoDBCollectionKey, List<DBObject>>) TransactionSynchronizationManager.getResource(mongoKey);
	}
	
	/**
	 * @return
	 */
	private boolean transactionActive() {
		return TransactionSynchronizationManager.isActualTransactionActive();
	}

	
	private static class MongoDBCollectionKey{
		private String db;
		private String collection;
		private WriteConcern writeConcern;
		
		protected MongoDBCollectionKey(String db, String collection, WriteConcern writeConcern) {
			this.db = db;
			this.collection = collection;
			this.writeConcern = writeConcern;
		}

		protected String getDb() {
			return db;
		}

		protected String getCollection() {
			return collection;
		}

		protected WriteConcern getWriteConcern() {
			return writeConcern;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result
					+ ((collection == null) ? 0 : collection.hashCode());
			result = prime * result + ((db == null) ? 0 : db.hashCode());
			result = prime * result
					+ ((writeConcern == null) ? 0 : writeConcern.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			MongoDBCollectionKey other = (MongoDBCollectionKey) obj;
			if (collection == null) {
				if (other.collection != null)
					return false;
			} else if (!collection.equals(other.collection))
				return false;
			if (db == null) {
				if (other.db != null)
					return false;
			} else if (!db.equals(other.db))
				return false;
			if (writeConcern == null) {
				if (other.writeConcern != null)
					return false;
			} else if (!writeConcern.equals(other.writeConcern))
				return false;
			return true;
		}
		
	}

}
