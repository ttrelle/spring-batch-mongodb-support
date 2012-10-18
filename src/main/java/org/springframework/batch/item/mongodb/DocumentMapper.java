package org.springframework.batch.item.mongodb;

import com.mongodb.DBObject;

/**
 * This mapper defines how MongoDB documents are mapped to business objects.
 * 
 * @author Tobias Trelle
 *
 * @param <T> Type of the mapped business object.
 */
public interface DocumentMapper<T> {

	/**
	 * Mapping of a single document.
	 * @param document The JSON object.
	 * @return The mapped business object.
	 */
	T map(DBObject document);
	
}
