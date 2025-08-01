package net.heeheehub.mysqlrepository.MySQLRepository.repo;

import java.sql.SQLException;

public class SQLTransaction {
	
	private Database database;
	
	private boolean active = false;
	
	public SQLTransaction(Database database) throws SQLException {
		this.database = database;
	}
	
	public void begin() {
		if(active) {
			throw new IllegalStateException("Transaction is already active");
		}
		active = true;
	}
	
	public void commit() throws SQLException {
		if(!active) throw new IllegalStateException("No active transaction");
		
		try {
			database.getConn().commit();
		} finally {
			active = false;
		}
	}
	
	public void rollback() throws SQLException {
		if(!active) throw new IllegalStateException("No active transaction");
		
		try {
			database.getConn().rollback();
		} finally {
			active = false;
		}
	}
	
	

}
