package org.springframework.batch.item.mongodb;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.springframework.transaction.support.TransactionSynchronizationAdapter;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.WriteConcern;

/**
 * Delays actually writing to a MongoDB if a transaction is active.
 * 
 * @author tobias.flohre
 *
 */
public class TransactionAwareMongoDBWriter {
	
	private static final String MONGO_KEY_PREFIX = TransactionAwareMongoDBWriter.class.getName() + ".MONGO_KEY";

	private Mongo mongo;
	private String mongoKey;
	
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
			mongo.getDB(db).getCollection(collection).insert(objectsToWrite, writeConcern);
		}
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
					catch (Exception e) {
						throw new MongoDBInsertFailedException("Could not write to Mongo DB", e);
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
						mongo.getDB(entry.getKey().getDb()).getCollection(entry.getKey().getCollection()).insert(entry.getValue(), entry.getKey().getWriteConcern());
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
		
	}
}
