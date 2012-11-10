package org.springframework.batch.item.mongodb;

import org.springframework.batch.item.file.LineMapper;

import com.mongodb.DBObject;
import com.mongodb.util.JSON;

/**
 * This line mapper converts a JSON string to a {@link DBObject}. 
 * 
 * @author Tobias Trelle
 */
public class JsonLineMapper implements LineMapper<DBObject> {

	@Override
	public DBObject mapLine(String line, int lineNumber) throws Exception {
		return (DBObject)JSON.parse(line);
	}

}
