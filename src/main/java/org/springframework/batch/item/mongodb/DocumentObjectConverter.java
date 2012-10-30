package org.springframework.batch.item.mongodb;

import com.mongodb.DBObject;

/**
 * This mapper defines how MongoDB documents are mapped to business objects.
 * <p/>
 * As soon as Spring Batch switches to Spring Core 3.x, this class can be
 * deleted its usage replaced by a standard Converter.
 * 
 * @author Tobias Trelle
 *
 * @param <T> Type of the mapped business object.
 */
public interface DocumentObjectConverter<T> {

	/**
	 * Mapping of a single document.
	 * @param document The JSON object.
	 * @return The mapped business object.
	 */
	T map(DBObject document);
	
}
