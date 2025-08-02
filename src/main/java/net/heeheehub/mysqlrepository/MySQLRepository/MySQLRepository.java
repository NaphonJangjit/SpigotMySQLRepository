package net.heeheehub.mysqlrepository.MySQLRepository;

import java.util.HashMap;
import java.util.Map;

import org.bukkit.Bukkit;
import org.bukkit.plugin.java.JavaPlugin;

import net.heeheehub.mysqlrepository.MySQLRepository.repo.Database;
import net.md_5.bungee.api.ChatColor;

public class MySQLRepository extends JavaPlugin {
	
	private final static Map<String, Database> databases = new HashMap<>();
	private static String mainDb;
	public static MySQLRepository instance;
	@Override
	public void onEnable() {
		saveDefaultConfig();
		if (!getConfig().isConfigurationSection("main") || getConfig().getConfigurationSection("main").getKeys(false).isEmpty()){
			getConfig().set("main.name", "mytestdatabase");
			getConfig().set("main.host", "localhost");
			getConfig().set("main.port", "3306");
			getConfig().set("main.user", "admin");
			getConfig().set("main.password", "1234");
			saveConfig();
		}
		instance = this;
		mainDb = getConfig().getString("main.name");
		registerGlobalDatabase(
				new Database(
						getConfig().getString("main.host"), 
						getConfig().getInt("main.port"), 
						mainDb, 
						getConfig().getString("main.user"), 
						getConfig().getString("main.password")
						)
				);
		for(String l : getConfig().getConfigurationSection("db.").getKeys(false)) {
			registerGlobalDatabase(
					new Database(
							getConfig().getString("db." + l + ".host"), 
							getConfig().getInt("db." + l + ".port"), 
							getConfig().getString("db." + l + ".name"), 
							getConfig().getString("db." + l + ".user"), 
							getConfig().getString("db." + l + ".password")
							)
					);
			Bukkit.getConsoleSender().sendMessage(ChatColor.translateAlternateColorCodes('&', "&7[&aMySQLRepository&7] &aRegistered " + getConfig().getString("db." + l + ".name")));
		}
		getMainDatabase().connect();
		Bukkit.getConsoleSender().sendMessage(ChatColor.translateAlternateColorCodes('&', "&7[&aMySQLRepository&7] &aConnected to mysql db" + mainDb));
	
		
		Bukkit.getConsoleSender().sendMessage(ChatColor.translateAlternateColorCodes('&', "&7[&aMySQLRepository&7] &aMySQLRepo enabled"));
		
	}
	
	@Override
	public void onDisable() {
		Bukkit.getConsoleSender().sendMessage(ChatColor.translateAlternateColorCodes('&', "&7[&aMySQLRepository&7] &cMySQLRepo disabled"));
	}
	
	public static Database getDatabase(String dbName) {
		return databases.get(dbName);
	}
	
	public static void registerGlobalDatabase(Database database) {
		databases.put(database.getDbName(), database);
		String dbName = database.getDbName();
		instance.getConfig().set("db." + dbName + ".name", dbName);
		instance.getConfig().set("db." + dbName + ".host", database.getHost());
		instance.getConfig().set("db." + dbName + ".port", database.getPort());
		instance.getConfig().set("db." + dbName + ".user", database.getUser());
		instance.getConfig().set("db." + dbName + ".password", database.getPassword());
		instance.saveConfig();
	
	}
	public static Database getMainDatabase() {
		return databases.get(mainDb);
	}
	
}
