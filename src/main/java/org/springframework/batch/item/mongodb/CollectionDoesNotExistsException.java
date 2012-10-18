package org.springframework.batch.item.mongodb;

import org.springframework.dao.DataAccessException;

public class CollectionDoesNotExistsException extends DataAccessException{

	public CollectionDoesNotExistsException(String msg) {
		super(msg);
	}

}
