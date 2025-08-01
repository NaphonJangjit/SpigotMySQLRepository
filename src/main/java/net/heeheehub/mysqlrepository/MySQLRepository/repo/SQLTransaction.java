package net.heeheehub.mysqlrepository.MySQLRepository.repo;

import java.sql.SQLException;

public class SQLTransaction {
	
	private Database database;
	
	private boolean active = false;
	
	public SQLTransaction(Database database) throws SQLException {
		this.database = database;
	}
	
	void begin() throws SQLException {
		if(active) {
			throw new IllegalStateException("Transaction is already active");
		}
		database.getConn().setAutoCommit(false);
		active = true;
	}
	
	void end() throws SQLException {
		active = false;
		database.getConn().setAutoCommit(true);
	}
	
	public void commit() throws SQLException {
		if(!active) throw new IllegalStateException("No active transaction");
		
		try {
			database.getConn().commit();
		} finally {
			reset();
		}
	}
	
	public void rollback() throws SQLException {
		if(!active) throw new IllegalStateException("No active transaction");
		
		try {
			database.getConn().rollback();
		} finally {
			reset();
		}
	}
	
	private void reset() throws SQLException {
		database.getConn().setAutoCommit(true);
		active = false;
	}
	
	public boolean isActive() {
		return active;
	}
	
	

}
