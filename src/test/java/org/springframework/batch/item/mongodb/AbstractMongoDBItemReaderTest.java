package org.springframework.batch.item.mongodb;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;

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
public abstract class AbstractMongoDBItemReaderTest {

	private static final String MONGOD_HOST = System.getProperty("host", "localhost");
	
	private static final int MONGOD_PORT = Integer.parseInt(System.getProperty("port", "27017"));
	
	private static final String DB_NAME = "test";
	
	private static final String COLLECTION_NAME = "reader";
	
	/** Unit under test. */
	protected MongoDBCursorItemReader reader;
	
	protected Mongo mongod;
	
	protected DBCollection collection;
	
	@Before
	public void setUp() throws UnknownHostException {
		// set up collection
		mongod = new Mongo(MONGOD_HOST, MONGOD_PORT);
		collection = mongod.getDB(DB_NAME).createCollection(COLLECTION_NAME, null);
		// create an empty collection requires an insert and a remove
		collection.insert(new BasicDBObject());
		collection.remove(new BasicDBObject());
		
		// prepare unit under test
		reader = new MongoDBCursorItemReader();
		reader.setMongo(mongod);
		reader.setDb(DB_NAME);
		reader.setCollection(COLLECTION_NAME);
	}
	
	@After
	public void tearDown() throws Exception {
		reader.doClose();
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
	
	protected List<DBObject> readAll() throws Exception {
		List<DBObject> docs = new ArrayList<DBObject>();
		
		DBObject doc;
		while ( (doc=(DBObject) reader.doRead()) != null ) {
			docs.add(doc);
		}		
		
		return docs;
	}
	
}
