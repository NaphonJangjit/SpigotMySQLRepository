package net.heeheehub.mysqlrepository.MySQLRepository.repo;


import org.bukkit.Bukkit;
import org.bukkit.ChatColor;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Database {

    private String host;
    private int port;
    private String dbName;
    private String user;
    private String password;
    private Connection conn;
    private String url;
    public Database(String host, int port, String dbName, String user, String password){
        this.host = host;
        this.port = port;
        this.dbName = dbName;
        this.user = user;
        this.password = password;
        this.conn = null;
        this.url = "jdbc:mysql://" + host + ":" + port + "/" + dbName;
    }

    public void connect(){
        try {
            this.conn = DriverManager.getConnection(url, user, password);
            Bukkit.getConsoleSender().sendMessage(ChatColor.translateAlternateColorCodes('&', "&7[&aSQLRepo&7] &aConnected to MySQL successfully!"));
        }catch (SQLException ex){
            throw new RuntimeException("Failed to connect to the database (" + dbName + "): " + ex.getMessage(), ex);
        }
    }


    public void disconnect(){
        try {
            conn.close();
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    public Connection getConn() {
        return conn;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getDbName() {
        return dbName;
    }

    public String getPassword() {
        return password;
    }

    public String getUser() {
        return user;
    }
}
