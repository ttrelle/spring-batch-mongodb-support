package org.springframework.batch.item.mongodb;

import java.net.UnknownHostException;

import static junit.framework.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.batch.item.mongodb.DocumentMapper;
import org.springframework.batch.item.mongodb.MongoDBRawItemReader;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.WriteConcern;
import com.mongodb.util.JSON;

/**
 * Tests for {@link MongoDBRawItemReader}.
 * <p/>
 * This test assumes that a mongod instance is running on localhost at the default port 27017. 
 * If you want to use other values, use VM parameters -Dhost=... and -Dport=...
 * 
 * @author Tobias Trelle
 */
public class MongoDBRawItemReaderTest {

	private static final String MONGOD_HOST = System.getProperty("host", "localhost");
	
	private static final int MONGOD_PORT = Integer.parseInt(System.getProperty("port", "27017"));
	
	private static final String DB_NAME = "test";
	
	private static final String COLLECTION_NAME = "reader";
	
	/** Unit under test. */
	private MongoDBRawItemReader reader;
	
	private Mongo mongod;
	
	private DBCollection collection;
	
	@Before
	public void setUp() throws UnknownHostException {
		// set up collection
		mongod = new Mongo(MONGOD_HOST, MONGOD_PORT);
		collection = mongod.getDB(DB_NAME).createCollection(COLLECTION_NAME, null);
		// create an empty collection requires an insert and a remove
		collection.insert(new BasicDBObject());
		collection.remove(new BasicDBObject());
		
		// prepare unit under test
		reader = new MongoDBRawItemReader();
		reader.setMongo(mongod);
		reader.setDb(DB_NAME);
		reader.setCollection(COLLECTION_NAME);

	}
	
	@Test(expected = CollectionDoesNotExistsException.class)
	public void should_fail_on_non_existing_collection() throws Exception {
		// given
		reader.setCollection("DOES_NOT_EXIST");
		
		// when
		reader.doOpen();
		
		// then: throw exception
	}

	@Test
	public void should_handle_empty_collection() throws Exception {
		// when
		reader.doOpen();
		DBObject o = reader.doRead();
		
		// then
		assertNull(o);
	}
	
	@Test
	public void should_read_all_documents() throws Exception {
		// given
		for (int i =0; i<5;i++) {
			insert("{i:" + i + ", j:42}");
		}
		
		// when
		reader.doOpen();
		
		// then
		assertReadCount(5);
	}
	
	@After
	public void tearDown() throws Exception {
		reader.doClose();
		//mongod.getDB(DB_NAME).getCollection(COLLECTION_NAME).drop();
		mongod.close();
	}
	
	private void insert(String json) {
		collection.insert((DBObject)JSON.parse(json), WriteConcern.NORMAL);
	}
	
	private void assertReadCount(int expected) throws Exception {
		// when
		int count = 0;
		while ( reader.doRead() != null ) {
			count++;
		}
		
		// then
		assertEquals("Document count mismatch", expected, count);		
	}
	
}
