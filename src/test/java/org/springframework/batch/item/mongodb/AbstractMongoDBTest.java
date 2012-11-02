package org.springframework.batch.item.mongodb;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.WriteConcern;
import com.mongodb.util.JSON;

/**
 * Base class for MongoDB driven tests.
 * <p/>
 * This test assumes that a mongod instance is running on localhost at the default port 27017. 
 * If you want to use other values, use VM parameters -Dhost=... and -Dport=...
 * 
 * @author Tobias Trelle
 */
public abstract class AbstractMongoDBTest {

	private static final String MONGOD_HOST = System.getProperty("host", "localhost");
	
	private static final int MONGOD_PORT = Integer.parseInt(System.getProperty("port", "27017"));
	
	protected static final String DB_NAME = "test";
	
	protected static final String COLLECTION_NAME = "user";
	
	protected Mongo mongod;
	
	protected DBCollection collection;
	
	protected void setUpMongo() throws UnknownHostException {
		// set up collection
		mongod = new Mongo(MONGOD_HOST, MONGOD_PORT);
		collection = mongod.getDB(DB_NAME).createCollection(COLLECTION_NAME, null);
		// create an empty collection requires an insert and a remove
		collection.insert(new BasicDBObject());
		collection.remove(new BasicDBObject());		
	}
	
	protected void tearDownMongo() {
		mongod.getDB(DB_NAME).getCollection(COLLECTION_NAME).drop();
		mongod.close();
	}
	
	protected void insert(String json) {
		collection.insert((DBObject)JSON.parse(json), WriteConcern.NORMAL);
	}
	
	protected static void assertReadCount(List<DBObject> docs, int expected) throws Exception {

		assertNotNull("List is <null>", docs);
		assertEquals("Document count mismatch", expected, docs.size());		
	}
	
	protected static void assertFields(DBObject doc, String... keys) {
		assertNotNull("Document is <null>", doc);
		for (String key: keys) {
			assertNotNull( "Field not read: " + doc.get(key) );
		}
		
		assertEquals("Key count mismatch", keys.length, doc.keySet().size());
	}

	protected static void assertFields(List<DBObject> docs, String... keys) {
		assertNotNull("Document list is <null>", docs);
		
		for (DBObject doc: docs) {
			assertFields(doc,keys);
		}
	}

	protected static List<DBObject> createDocuments(int n) {
		List<DBObject> docs = new ArrayList<DBObject>();
		
		for (int i=0; i<n;i++) {
			docs.add( new BasicDBObject("i", i));
		}
		
		return docs;
	}
	
	protected void assertCollectionCount(long expected) {
		assertThat(collection.count() ,is(expected));
	}
	
	protected class User {
		private String _id;
		private String name;
		private int loginCount;
		public String getId() {
			return _id;
		}
		public void setId(String _id) {
			this._id = _id;
		}
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
		public int getLoginCount() {
			return loginCount;
		}
		public void setLoginCount(int loginCount) {
			this.loginCount = loginCount;
		}
		
	}
	
}
