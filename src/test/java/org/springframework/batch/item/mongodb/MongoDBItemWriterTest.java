package org.springframework.batch.item.mongodb;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.core.convert.converter.Converter;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * Tests for {@link MongoDBItemWriter}.
 * The transactional tests test the transactional version of the writer without an active transaction.
 * <p/>
 * This test assumes that a mongod instance is running on localhost at the default port 27017. 
 * If you want to use other values, use VM parameters -Dhost=... and -Dport=...
 * 
 * @author Tobias Trelle
 */
public class MongoDBItemWriterTest extends AbstractMongoDBTest {

	/** Unit under test. */
	protected MongoDBItemWriter writer;		
	
	@Before
	public void setUp() throws Exception {
		setUpMongo();
		
		// prepare unit under test
		writer = new MongoDBItemWriter();
		writer.setMongo(mongod);
		writer.setDb(DB_NAME);
		writer.setCollection(COLLECTION_NAME);
		writer.afterPropertiesSet();
	}	
	
	@Test
	public void should_write_nulled_list_transactional() throws Exception {
		// when
		writer.write(null);
		
		// then
		assertCollectionCount(0);
	}

	@Test
	public void should_write_empty_list_transactional() throws Exception {
		// when
		writer.write( new ArrayList<DBObject>() );
		
		// then
		assertCollectionCount(0);
	}

	@Test
	public void should_write_one_document_transactional() throws Exception {
		// when
		writer.write( createDocuments(1) );
		
		// then
		assertCollectionCount(1);
	}

	@Test
	public void should_write_multiple_documents_transactional() throws Exception {
		// when
		writer.write( createDocuments(10) );
		
		// then
		assertCollectionCount(10);
	}

	@Test
	public void should_write_converted_documents_transactional() throws Exception {
		// given
		writer.setConverter( new ObjectUserConverter() );
		List<User> users = createUsers();
		
		// when
		writer.write( users );
		
		// then
		assertCollectionCount(3);
	}

	@Test
	public void should_write_nulled_list() throws Exception {
		writer.setTransactional(false);
		// when
		writer.write(null);
		
		// then
		assertCollectionCount(0);
	}

	@Test
	public void should_write_empty_list() throws Exception {
		writer.setTransactional(false);
		// when
		writer.write( new ArrayList<DBObject>() );
		
		// then
		assertCollectionCount(0);
	}

	@Test
	public void should_write_one_document() throws Exception {
		writer.setTransactional(false);
		// when
		writer.write( createDocuments(1) );
		
		// then
		assertCollectionCount(1);
	}

	@Test
	public void should_write_multiple_documents() throws Exception {
		writer.setTransactional(false);
		// when
		writer.write( createDocuments(10) );
		
		// then
		assertCollectionCount(10);
	}

	@Test
	public void should_write_converted_documents() throws Exception {
		writer.setTransactional(false);
		// given
		writer.setConverter( new ObjectUserConverter() );
		List<User> users = createUsers();
		
		// when
		writer.write( users );
		
		// then
		assertCollectionCount(3);
	}

	@Test(expected = IllegalArgumentException.class)
	public void should_detect_missing_converter() throws Exception {
		// given
		List<User> users = createUsers();
		
		// when
		writer.write( users );
		
		// then: expect exception
	}
	
	@Test(expected = IllegalArgumentException.class) 
	public void should_detect_missing_mongo_property() throws Exception {
		// given
		writer.setMongo(null);
		
		// when
		writer.afterPropertiesSet();
		
		// then: expect exception
	}

	@Test(expected = IllegalArgumentException.class) 
	public void should_detect_missing_db_property() throws Exception {
		// given
		writer.setDb(null);
		
		// when
		writer.afterPropertiesSet();
		
		// then: expect exception
	}

	@Test(expected = IllegalArgumentException.class) 
	public void should_detect_empty_db_property() throws Exception {
		// given
		writer.setDb("");
		
		// when
		writer.afterPropertiesSet();
		
		// then: expect exception
	}
	
	@Test(expected = IllegalArgumentException.class) 
	public void should_detect_missing_collection_property() throws Exception {
		// given
		writer.setCollection(null);
		
		// when
		writer.afterPropertiesSet();
		
		// then: expect exception
	}

	@Test(expected = IllegalArgumentException.class) 
	public void should_detect_empty_collection_property() throws Exception {
		// given
		writer.setCollection("");
		
		// when
		writer.afterPropertiesSet();
		
		// then: expect exception
	}
	
	@After
	public void tearDown() throws Exception {
		tearDownMongo();
	}		
	

	private List<User> createUsers() {
		List<User> users = new ArrayList<User>();
		for (int i=0;i<3;i++) {
			User user = new User();
			
			user.setId(""+i);
			user.setId("User " +i);
			user.setLoginCount(i*i);
			users.add(user);
		}
		return users;
	}	
	
	private class ObjectUserConverter implements Converter<Object, DBObject> {

		@Override
		public DBObject convert(Object o) {
			User usr = (User)o;
			DBObject doc = new BasicDBObject();
			
			doc.put("_id", usr.getId());
			doc.put("name", usr.getName());
			doc.put("n", usr.getLoginCount());
			
			return doc;
		}
		
	}
	
}
