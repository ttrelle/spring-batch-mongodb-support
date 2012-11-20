package org.springframework.batch.item.mongodb;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.core.convert.converter.Converter;

import com.mongodb.DBObject;

/**
 * Tests for {@link MongoDBItemReader}.
 * <p/>
 * This test assumes that a mongod instance is running on localhost at the default port 27017. 
 * If you want to use other values, use VM parameters -Dhost=... and -Dport=...
 * 
 * @author Tobias Trelle
 */
public class MongoDBItemReaderTest extends AbstractMongoDBTest {

	/** Unit under test. */
	protected MongoDBItemReader reader;	
	
	@Before
	public void setUp() throws UnknownHostException {
		setUpMongo();
		
		// prepare unit under test
		reader = new MongoDBItemReader();
		reader.setMongo(mongod);
		reader.setDb(DB_NAME);
		reader.setCollection(COLLECTION_NAME);
	}	
	
	@Test(expected = IllegalArgumentException.class)
	public void should_fail_on_non_existing_database() throws Exception {
		// given
		reader.setDb("DOES_NOT_EXIST");
		
		// when
		reader.doOpen();
		
		// then: expect exception
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void should_fail_on_non_existing_collection() throws Exception {
		// given
		reader.setCollection("DOES_NOT_EXIST");
		
		// when
		reader.doOpen();
		
		// then: expect exception
	}

	@Test
	public void should_handle_empty_collection() throws Exception {
		// when
		reader.doOpen();
		DBObject o = (DBObject)reader.doRead();
		
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
		List<DBObject> docs = readAll();
		
		// then
		assertReadCount(docs, 5);
		assertFields(docs, "_id", "i", "j");
	}
	
	@Test
	public void should_read_all_documents_with_a_subset_of_keys() throws Exception {
		// given
		for (int i =0; i<5;i++) {
			insert("{i:" + i + ", j:42}");
		}
		reader.setKeys("{i:1,_id:0}");
	
		// when
		reader.doOpen();
		List<DBObject> docs = readAll();
		
		// then
		assertReadCount(docs, 5);
		assertFields(docs, "i");
	}	
	
	@Test(expected = IllegalArgumentException.class)
	public void should_handle_illegal_key_set() throws Exception {
		// given
		for (int i =0; i<5;i++) {
			insert("{i:" + i + ", j:42}");
		}
		reader.setKeys("{i:1 _id:0}");
	
		// when
		reader.doOpen();
		
		// then: expect expection
	}
	
	@Test
	public void should_query_documents() throws Exception {
		// given
		for (int i =0; i<5;i++) {
			insert("{i:" + i + ", j:42}");
		}
		reader.setQuery("{i:{ $gt: 2 }}");
	
		// when
		reader.doOpen();
		List<DBObject> docs = readAll();
		
		// then
		assertReadCount(docs, 2);
		assertFields(docs, "_id", "i", "j");
	}

	@Test
	public void should_query_documents_with_limit() throws Exception {
		// given
		for (int i =0; i<5;i++) {
			insert("{i:" + i + ", j:42}");
		}
		reader.setLimit(3);
	
		// when
		reader.doOpen();
		List<DBObject> docs = readAll();
		
		// then
		assertReadCount(docs, 3);
		assertFields(docs, "_id", "i", "j");
	}
	
	
	@Test
	public void should_query_documents_with_a_subset_of_keys() throws Exception {
		// given
		for (int i =0; i<5;i++) {
			insert("{i:" + i + ", j:42}");
		}
		reader.setQuery("{i:{ $gt: 2 }}");
		reader.setKeys("{j:1,_id:0}");
	
		// when
		reader.doOpen();
		List<DBObject> docs = readAll();
		
		// then
		assertReadCount(docs, 2);
		assertFields(docs, "j");
	}
	
	
	@Test(expected = IllegalArgumentException.class)
	public void should_handle_illegal_query() throws Exception {
		// given
		for (int i =0; i<5;i++) {
			insert("{i:" + i + ", j:42}");
		}
		reader.setQuery("{i: $gt: 2 }}");
	
		// when
		reader.doOpen();
		
		// then: expect expection
	}
	
	@Test
	public void should_use_document_converter() throws Exception {
		// given
		insert("{_id:\"0\", name: \"User 1\", n:3}");
		reader.setConverter(new DocumentUserConverter());
		
		// when
		reader.doOpen();
		Object o = reader.doRead();
		
		// then
		assertThat(o instanceof User, is(true));
		User u = (User)o;
		assertThat(u.getId(), is("0"));
		assertThat(u.getName(), is("User 1"));
		assertThat(u.getLoginCount(), is(3));
	}
	
	@Test
	public void should_sort_documents() throws Exception {
		// given
		for (int i =0; i<5;i++) {
			insert("{i:" + i + ", j:42}");
		}
		reader.setSort("{i:-1}");
	
		// when
		reader.doOpen();
		List<DBObject> docs = readAll();
		
		// then
		assertReadCount(docs, 5);
		assertThat( (Integer)docs.get(0).get("i"), is(4) );
		assertThat( (Integer)docs.get(4).get("i"), is(0) );
	}

	@Test(expected = IllegalArgumentException.class)
	public void should_detect_illegal_sort() throws Exception {
		// given
		for (int i =0; i<5;i++) {
			insert("{i:" + i + ", j:42}");
		}
		reader.setSort("not a JSON document");
	
		// when
		reader.doOpen();
		
		// then: expect expection		
	}
	
	@Test(expected = IllegalArgumentException.class)
	public void should_handle_negative_jump_value() throws Exception {
		// given
		for (int i =0; i<5;i++) {
			insert("{i:" + i + ", j:42}");
		}
		
		// when
		reader.doOpen();
		reader.jumpToItem(-1);
	}

	@Test
	public void should_jumpTo_first_position() throws Exception {
		// given
		for (int i =0; i<5;i++) {
			insert("{i:" + i + ", j:42}");
		}
		
		// when
		reader.doOpen();
		reader.jumpToItem(0);
		List<DBObject> docs = readAll();
		
		// then
		assertReadCount(docs, 5);
	}

	@Test
	public void should_jumpTo_last_position() throws Exception {
		// given
		for (int i =0; i<5;i++) {
			insert("{i:" + i + ", j:42}");
		}
		
		// when
		reader.doOpen();
		reader.jumpToItem(4);
		List<DBObject> docs = readAll();
		
		// then
		assertReadCount(docs, 1);
	}
	
	
	@Test
	public void should_jumpTo_unreachable_position() throws Exception {
		// given
		for (int i =0; i<5;i++) {
			insert("{i:" + i + ", j:42}");
		}
		
		// when
		reader.doOpen();
		reader.jumpToItem(1000);
		List<DBObject> docs = readAll();
		
		// then
		assertReadCount(docs, 0);
	}	
	
	@Test(expected = IllegalArgumentException.class) 
	public void should_detect_missing_mongo_property() throws Exception {
		// given
		reader.setMongo(null);
		
		// when
		reader.afterPropertiesSet();
		
		// then: expect exception
	}

	@Test(expected = IllegalArgumentException.class) 
	public void should_detect_missing_db_property() throws Exception {
		// given
		reader.setDb(null);
		
		// when
		reader.afterPropertiesSet();
		
		// then: expect exception
	}

	@Test(expected = IllegalArgumentException.class) 
	public void should_detect_empty_db_property() throws Exception {
		// given
		reader.setDb("");
		
		// when
		reader.afterPropertiesSet();
		
		// then: expect exception
	}
	
	@Test(expected = IllegalArgumentException.class) 
	public void should_detect_missing_collection_property() throws Exception {
		// given
		reader.setCollection(null);
		
		// when
		reader.afterPropertiesSet();
		
		// then: expect exception
	}

	@Test(expected = IllegalArgumentException.class) 
	public void should_detect_empty_collection_property() throws Exception {
		// given
		reader.setCollection("");
		
		// when
		reader.afterPropertiesSet();
		
		// then: expect exception
	}
	
	@After
	public void tearDown() throws Exception {
		reader.doClose();
		tearDownMongo();
	}	
	
	protected List<DBObject> readAll() throws Exception {
		List<DBObject> docs = new ArrayList<DBObject>();
		
		DBObject doc;
		while ( (doc=(DBObject) reader.doRead()) != null ) {
			docs.add(doc);
		}		
		
		return docs;
	}	
	
	private class DocumentUserConverter implements Converter<DBObject, User> {

		@Override
		public User convert(DBObject document) {
			User usr = new User();
			
			usr.setId((String)document.get("_id"));
			usr.setName((String)document.get("name"));
			usr.setLoginCount((Integer)document.get("n"));
			
			return usr;
		}
		
	}
	
}
