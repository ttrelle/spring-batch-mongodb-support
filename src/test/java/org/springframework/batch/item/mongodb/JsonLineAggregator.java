package org.springframework.batch.item.mongodb;

import org.springframework.batch.item.file.transform.LineAggregator;

import com.mongodb.DBObject;

/**
 * This line aggregator converts a MongoDB {@link DBObject} to its
 * string representation (JSON format).
 * 
 * @author Tobias Trelle
 */
public class JsonLineAggregator implements LineAggregator<DBObject> {

	@Override
	public String aggregate(DBObject item) {
		return item != null ? item.toString() : null;
	}

}
