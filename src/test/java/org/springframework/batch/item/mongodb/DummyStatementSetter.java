package org.springframework.batch.item.mongodb;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.springframework.batch.item.database.ItemPreparedStatementSetter;

import com.mongodb.DBObject;

/**
 * This is needed to cause for the JDBC item writer to fail.
 * 
 * @author Tobias Trelle
 */
public class DummyStatementSetter implements ItemPreparedStatementSetter<DBObject> {

	@Override
	public void setValues(DBObject item, PreparedStatement ps)
			throws SQLException {
		ps.setInt(0, 42);
	}

}
