package org.springframework.batch.item.mongodb;

import org.springframework.dao.DataAccessException;

/**
 * This exception is thrown when a reader wants to reader document from
 * a collection that does not exist.
 * 
 * @author tobias.trelle
 */
public class CollectionDoesNotExistsException extends DataAccessException {

	public CollectionDoesNotExistsException(String msg) {
		super(msg);
	}

}
