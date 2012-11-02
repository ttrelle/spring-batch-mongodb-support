package org.springframework.batch.item.mongodb;

import com.mongodb.DBObject;

/**
 * This mapper defines how business objects are mapped to MongoDB documents.
 * <p/>
 * As soon as Spring Batch switches to Spring Core 3.x, this class can be
 * deleted its usage replaced by a standard Converter.
 * 
 * @author Tobias Trelle
 *
 * @param <T> Type of the mapped business object.
 */
public interface ObjectDocumentConverter {

	/**
	 * Mapping of a single object.
	 * @param object The business object to map.
	 * @return The mapped JSON object.
	 */
	DBObject convert(Object object);
	
}
