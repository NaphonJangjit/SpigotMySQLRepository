package net.heeheehub.mysqlrepository.MySQLRepository.object;

public @interface SQLForeignKey {
	String table();
	String attribute();
}
