package org.springframework.batch.support.mongodb;

import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.data.mongodb.core.MongoOperations;

/**
 * This reader reads items from a MongoDB collection.
 * @author Tobias Trelle
 *
 * @param <T> Type of mapped object.
 */
public class MongoDBItemReader<T> extends AbstractItemCountingItemStreamItemReader<T> {

	/** MongoDB template. */
	private MongoOperations template;
	
	/** Name of the collection to read from. */
	private String collection;
	
	/** 
	 * Query in JSON notation, e.g. <code>{a: 1, b: 2}</code> (optional).
	 * <p/>
	 * If no query is given, the whole collection is read.
	 */
	private String query;
	
	@Override
	protected void jumpToItem(int itemIndex) throws Exception {
		// TODO Auto-generated method stub
		// skip to right position
	}

	@Override
	protected T doRead() throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected void doOpen() throws Exception {
				// TODO Auto-generated method stub
		
	}

	@Override
	protected void doClose() throws Exception {
		// TODO Auto-generated method stub
		
	}

}
