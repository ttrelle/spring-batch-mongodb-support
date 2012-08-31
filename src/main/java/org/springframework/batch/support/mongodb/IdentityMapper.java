package org.springframework.batch.support.mongodb;

import com.mongodb.DBObject;

public class IdentityMapper implements DocumentMapper<DBObject> {

	@Override
	public DBObject map(DBObject document) {
		return document;
	}

}
