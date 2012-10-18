package org.springframework.batch.item.mongodb;

import org.springframework.core.NestedRuntimeException;

/**
 * This exception is thrown when a JSON string can not be parsed
 * into a DBObject.
 * 
 * @author tobias.trelle
 */
public class IllegalDocumentException extends NestedRuntimeException {

	public IllegalDocumentException(String msg, Throwable cause) {
		super(msg, cause);
	}
	
}
