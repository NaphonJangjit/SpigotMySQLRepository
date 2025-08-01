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
	@Override
	public void onEnable() {
		if(getConfig().contains("main")) {
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
			getMainDatabase().connect();
			Bukkit.getConsoleSender().sendMessage(ChatColor.translateAlternateColorCodes('&', "&7[&aMySQLRepository&7] &aConnected to mysql db" + mainDb));
		}
		
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
	}
	
	public static Database getMainDatabase() {
		return databases.get(mainDb);
	}
	
}
