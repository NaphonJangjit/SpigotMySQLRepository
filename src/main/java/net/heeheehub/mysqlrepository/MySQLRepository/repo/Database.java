package net.heeheehub.mysqlrepository.MySQLRepository.repo;


import org.bukkit.Bukkit;
import org.bukkit.ChatColor;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


/**
 * Manages the connection to a MySQL database.
 * <p>
 * This class handles the establishment, management, and termination of a database connection.
 * It provides methods to connect, disconnect, and retrieve the current connection object.
 * </p>
 *
 * @author Naphon
 * @version 1.0-SNAPSHOT
 */
public class Database {

    private String host;
    private int port;
    private String dbName;
    private String user;
    private String password;
    private Connection conn;
    private String url;
    /**
     * Constructs a new Database object with the specified connection details.
     *
     * @param host     The database host address.
     * @param port     The database port number.
     * @param dbName   The name of the database.
     * @param user     The username for the database.
     * @param password The password for the database user.
     */
    public Database(String host, int port, String dbName, String user, String password){
        this.host = host;
        this.port = port;
        this.dbName = dbName;
        this.user = user;
        this.password = password;
        this.conn = null;
        this.url = "jdbc:mysql://" + host + ":" + port + "/" + dbName;
    }

    /**
     * Establishes a connection to the database.
     * <p>
     * This method attempts to connect to the MySQL database using the provided
     * credentials. It logs a success message to the console upon a successful
     * connection.
     * </p>
     *
     * @throws RuntimeException if the connection fails.
     */
    public void connect(){
        try {
            this.conn = DriverManager.getConnection(url, user, password);
            Bukkit.getConsoleSender().sendMessage(ChatColor.translateAlternateColorCodes('&', "&7[&aSQLRepo&7] &aConnected to MySQL successfully!"));
        }catch (SQLException ex){
            throw new RuntimeException("Failed to connect to the database (" + dbName + "): " + ex.getMessage(), ex);
        }
    }

    /**
     * Closes the database connection.
     * <p>
     * This method safely closes the active connection and logs a disconnection message.
     * </p>
     *
     * @throws RuntimeException if an error occurs while closing the connection.
     */
    public void disconnect(){
        try {
            conn.close();
            Bukkit.getConsoleSender().sendMessage(ChatColor.translateAlternateColorCodes('&', "&7[&aSQLRepo&7] &cDisconnected from " + dbName + "!"));
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    /**
     * Creates and returns a new database connection instance without closing the old one.
     * <p>
     * This is useful for multi-threaded applications where each thread needs its own
     * dedicated connection.
     * </p>
     *
     * @return A new {@link Connection} object. Returns {@code null} if an error occurs.
     */
    public Connection newConnection() {
    	Connection conn = null;
		try {
			conn = DriverManager.getConnection(url, user, password);
		} catch (SQLException e) {
			e.printStackTrace();
		}
    	return conn;
    }

    /**
     * Retrieves the current database connection.
     *
     * @return The active {@link Connection} object.
     */
    public Connection getConn() {
        return conn;
    }
    /**
     * Retrieves the database host.
     *
     * @return The host address.
     */
    public String getHost() {
        return host;
    }

    /**
     * Retrieves the database port.
     *
     * @return The port number.
     */
    public int getPort() {
        return port;
    }

    /**
     * Retrieves the database name.
     *
     * @return The name of the database.
     */
    public String getDbName() {
        return dbName;
    }

    /**
     * Retrieves the database password.
     *
     * @return The password for the database user.
     */
    public String getPassword() {
        return password;
    }

    /**
     * Retrieves the database user.
     *
     * @return The username for the database.
     */
    public String getUser() {
        return user;
    }
}
