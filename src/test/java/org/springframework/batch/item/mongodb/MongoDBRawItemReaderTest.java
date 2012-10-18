package org.springframework.batch.item.mongodb;

import static junit.framework.Assert.assertNull;

import java.util.List;

import org.junit.Test;

import com.mongodb.DBObject;

/**
 * Tests for {@link MongoDBRawItemReader}.
 * <p/>
 * This test assumes that a mongod instance is running on localhost at the default port 27017. 
 * If you want to use other values, use VM parameters -Dhost=... and -Dport=...
 * 
 * @author Tobias Trelle
 */
public class MongoDBRawItemReaderTest extends AbstractMongoDBItemReaderTest {
	
	@Test(expected = CollectionDoesNotExistsException.class)
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
	
	@Test(expected = IllegalDocumentException.class)
	public void should_handle__illegal_key_set() throws Exception {
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
	
	
	@Test(expected = IllegalDocumentException.class)
	public void should_handle__illegal_query() throws Exception {
		// given
		for (int i =0; i<5;i++) {
			insert("{i:" + i + ", j:42}");
		}
		reader.setQuery("{i: $gt: 2 }}");
	
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
	public void should_detect_missing_collection_property() throws Exception {
		// given
		reader.setCollection(null);
		
		// when
		reader.afterPropertiesSet();
		
		// then: expect exception
	}
	
}
