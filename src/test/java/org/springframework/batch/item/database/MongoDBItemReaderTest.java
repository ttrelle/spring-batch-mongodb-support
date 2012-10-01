package org.springframework.batch.item.database;

import java.net.UnknownHostException;

import static junit.framework.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.batch.item.database.DocumentMapper;
import org.springframework.batch.item.database.MongoDBItemReader;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

/**
 * Tests for {@link MongoDBItemReader}.
 * <p/>
 * This test assumes that a mongod instance is running on localhost at the default port 27017. 
 * If you want to use other values, use VM parameters -Dhost=... and -Dport=...
 * 
 * @author Tobias Trelle
 */
public class MongoDBItemReaderTest {

	private static final String MONGOD_HOST = System.getProperty("host", "localhost");
	
	private static final int MONGOD_PORT = Integer.parseInt(System.getProperty("port", "27017"));
	
	private static final String DB_NAME = "test";
	
	private static final String COLLECTION_NAME = "reader";
	
	/** Unit under test. */
	private MongoDBItemReader<DomainObject> reader;
	
	private Mongo mongod;
	
	private DBCollection collection;
	
	@Before
	public void setUp() throws UnknownHostException {
		// set up collection
		mongod = new Mongo(MONGOD_HOST, MONGOD_PORT);
		collection = mongod.getDB(DB_NAME).createCollection(COLLECTION_NAME, null);
		
		// prepare unit under test
		reader = new MongoDBItemReader<MongoDBItemReaderTest.DomainObject>();
		reader.setMongo(mongod);
		reader.setDb(DB_NAME);
		reader.setCollection(COLLECTION_NAME);
		reader.setMapper(new DomainObjectMapper());
	}
	
	@Test(expected = RuntimeException.class)
	public void should_fail_on_empty_collection() throws Exception {
		// when
		reader.doOpen();
		DomainObject o = reader.doRead();
	}

	public void ashould_fail_on_empty_collection() throws Exception {
		// when
		reader.doOpen();
		DomainObject o = reader.doRead();
		
		// then
		assertNotNull(o);
	}
	
	
	@After
	public void tearDown() throws Exception {
		reader.doClose();
		mongod.getDB(DB_NAME).getCollection(COLLECTION_NAME).drop();
		mongod.close();
	}

	class DomainObject {
		private String id;
		private int index;
		
		
		public DomainObject(String id, int index) {
			super();
			this.id = id;
			this.index = index;
		}
		public void setId(String id) {
			this.id = id;
		}
		public void setIndex(int index) {
			this.index = index;
		}
	}
	
	class DomainObjectMapper implements DocumentMapper<DomainObject> {

		@Override
		public DomainObject map(DBObject document) {
			if (document == null) return null;

			return new DomainObject((String)document.get("id"), (Integer)document.get("index"));
		}
		
	}
	
}
