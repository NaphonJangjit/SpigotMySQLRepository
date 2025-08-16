package net.heeheehub.mysqlrepository.MySQLRepository.repo;

import java.sql.SQLException;

/**
 * Manages database transactions.
 * <p>
 * This class provides methods to begin, commit, and rollback a database transaction.
 * It ensures that database operations are atomic, consistent, isolated, and durable (ACID).
 * </p>
 *
 * @author Naphon
 * @version 1.0-SNAPSHOT
 */
public class SQLTransaction {
	
	private Database database;
	
	private boolean active = false;
	
	/**
     * Constructs an SQLTransaction instance.
     *
     * @param database The database connection object.
     * @throws SQLException If a database access error occurs.
     */
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
	
	/**
     * Commits the current transaction.
     * <p>
     * This method applies all changes made in the transaction to the database.
     * After a successful commit, the transaction is no longer active.
     * </p>
     *
     * @throws SQLException      If a database access error occurs.
     * @throws IllegalStateException If no transaction is currently active.
     */
	public void commit() throws SQLException {
		if(!active) throw new IllegalStateException("No active transaction");
		
		try {
			database.getConn().commit();
		} finally {
			reset();
		}
	}
	
	/**
     * Rolls back the current transaction.
     * <p>
     * This method discards all changes made in the transaction, restoring the
     * database to its state before the transaction began.
     * After a rollback, the transaction is no longer active.
     * </p>
     *
     * @throws SQLException      If a database access error occurs.
     * @throws IllegalStateException If no transaction is currently active.
     */
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
	
	/**
     * Checks if a transaction is currently active.
     *
     * @return {@code true} if a transaction is active, {@code false} otherwise.
     */
	public boolean isActive() {
		return active;
	}
	
	

}
