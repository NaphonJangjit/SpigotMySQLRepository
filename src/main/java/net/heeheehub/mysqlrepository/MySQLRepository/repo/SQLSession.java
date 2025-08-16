package net.heeheehub.mysqlrepository.MySQLRepository.repo;

import net.heeheehub.mysqlrepository.MySQLRepository.object.MySQLColumn;
import net.heeheehub.mysqlrepository.MySQLRepository.object.MySQLField;
import net.heeheehub.mysqlrepository.MySQLRepository.object.SQLForeignKey;
import net.heeheehub.mysqlrepository.MySQLRepository.object.SQLId;
import net.heeheehub.mysqlrepository.MySQLRepository.object.SQLPrimaryKey;

import java.lang.instrument.IllegalClassFormatException;
import java.lang.reflect.Field;
import java.sql.*;
import java.util.*;


/**
 * Manages database sessions, transactions, and object persistence.
 * <p>
 * This class acts as a central point for interacting with a MySQL database.
 * It provides methods for persisting, retrieving, updating, and deleting objects
 * that are mapped to database tables. It also manages database connections and transactions.
 * </p>
 *
 * @author Naphon
 * @version 1.0-SNAPSHOT
 */
public class SQLSession {

    private Database database;
    private Map<String, Object> persistenceContext;
    private SQLTransaction tx;
    private boolean isClosed;
    
    
    /**
     * Constructs a new SQLSession with a given database connection.
     * This constructor establishes a connection to the database and initializes
     * the persistence context and transaction manager.
     *
     * @param database The database object containing connection details.
     * @throws SQLException if a database access error occurs.
     */
    public SQLSession(Database database) throws SQLException {
        this.database = new Database(database.getHost(), database.getPort(), database.getDbName(), database.getUser(), database.getPassword());
        this.database.connect();
        this.isClosed = false;
        this.persistenceContext = new HashMap<>();
        this.tx = new SQLTransaction(database);
    }
    
    /**
     * Starts a new database transaction.
     *
     * @return The {@link SQLTransaction} object managing the transaction.
     * @throws SQLException if a database access error occurs.
     */
    public SQLTransaction beginTransaction() throws SQLException {
    	tx.begin();
    	return tx;
    }
    
    /**
     * Ends the current transaction.
     *
     * @throws SQLException if a database access error occurs.
     */
    public void endTransaction() throws SQLException {
    	tx.end();
    }

    
    /**
     * Persists a given object to the database.
     * <p>
     * If the object has an existing ID and is already in the persistence context,
     * this method updates its corresponding record in the database.
     * Otherwise, it inserts a new record.
     * </p>
     *
     * @param object The object to be persisted.
     * @return The ID of the persisted object.
     * @throws SQLException               if a database access error occurs.
     * @throws IllegalClassFormatException if the object's class has multiple ID fields.
     * @throws IllegalAccessException     if the application cannot access the fields of the object.
     * @throws IllegalStateException      if the session is closed or there is no active transaction.
     */
    public Long persists(Object object) throws SQLException, IllegalClassFormatException, IllegalAccessException {
        if(isClosed) throw new IllegalStateException("Session is closed.");
    	if(!tx.isActive()) throw new IllegalStateException("No active transaction");
        Long id = getId(object);
        String key;
        if(id != null) {
	        key = getKey(object, id);
	        if(persistenceContext.containsKey(key)){
	            update(object);
	            return id;
	        }
        }
        String tableName = getTableName(object);

        Map<String, Field> params = getColumnData(object, true);
        List<String> colNameList = new ArrayList<>(params.keySet());
        StringBuilder codeBuilder = new StringBuilder("INSERT INTO ").
                append(tableName).append(" (")
                .append(String.join(", ", colNameList))
                .append(") VALUES (")
                .append("?,".repeat(colNameList.size()));

        codeBuilder.setLength(codeBuilder.length()-1);
        codeBuilder.append(")");

        try(PreparedStatement ps = database.getConn().prepareStatement(codeBuilder.toString(), Statement.RETURN_GENERATED_KEYS)){
            for (int i = 0; i < colNameList.size(); i++) {
                Field field = params.get(colNameList.get(i));
                field.setAccessible(true);
                Object value = field.get(object);
                if(value instanceof UUID uuid) {
                	ps.setObject(i + 1, uuid.toString());
                }else ps.setObject(i + 1, value);
            }

            ps.executeUpdate();

            for (Field field : object.getClass().getDeclaredFields()) {
                if (field.isAnnotationPresent(SQLId.class)) {
                    field.setAccessible(true);
                    try (ResultSet generatedKeys = ps.getGeneratedKeys()) {
                        if (generatedKeys.next()) {
                            field.set(object, generatedKeys.getLong(1));
                        }
                    }
                    break;
                }
            }

            id = getId(object);
            key = getKey(object, id);
            persistenceContext.put(key, object);
        }catch (SQLException ex){
            if(ex.getErrorCode() == 1146){
                createTable(tableName, object.getClass());
                return persists(object);
            }else {
                throw new RuntimeException(ex);
            }
        }
        
        return id;
    }

    
    /**
     * Retrieves an object from the database by its ID.
     *
     * @param clazz The class of the object to retrieve.
     * @param id    The ID of the object.
     * @param <T>   The type of the object.
     * @return The retrieved object, or null if not found.
     * @throws IllegalStateException if the session is closed.
     * @throws RuntimeException      if the class has no {@code @SQLId} field or a database error occurs.
     */
    public <T> T get(Class<T> clazz, Long id) {
    	if(isClosed) throw new IllegalStateException("Session is closed.");
        try {
            String tableName = getTableName(clazz);
            String idColumn = null;
            Field idField = null;

            for (Field field : clazz.getDeclaredFields()) {
                if (field.isAnnotationPresent(SQLId.class)) {
                    idField = field;
                    idField.setAccessible(true);

                    if (field.isAnnotationPresent(MySQLColumn.class)) {
                        idColumn = field.getAnnotation(MySQLColumn.class).value();
                    } else {
                        idColumn = field.getName();
                    }
                    break;
                }
            }

            if (idColumn == null || idField == null) {
                throw new RuntimeException("Class " + clazz.getSimpleName() + " has no @SQLId field");
            }

            String sql = "SELECT * FROM `" + tableName + "` WHERE `" + idColumn + "` = ? LIMIT 1";

            try (PreparedStatement ps = database.newConnection().prepareStatement(sql)) {
                ps.setObject(1, id);
                ResultSet rs = ps.executeQuery();

                if (!rs.next()) {
                    return null;
                }

                T instance = clazz.getDeclaredConstructor().newInstance();

                Map<String, Field> columnData = getColumnData(clazz, true);
                for (Map.Entry<String, Field> entry : columnData.entrySet()) {
                    String columnName = entry.getKey();
                    Field field = entry.getValue();
                    field.setAccessible(true);

                    Object value = rs.getObject(columnName);
                    field.set(instance, value);
                }

                if (!columnData.containsKey(idColumn)) {
                    Object value = rs.getObject(idColumn);
                    idField.set(instance, value);
                }

                persistenceContext.put(getKey(instance, id), instance);

                return instance;

            } catch (Exception e) {
                throw new RuntimeException("Failed to retrieve object", e);
            }

        } catch (Exception e) {
            throw new RuntimeException("Failed to execute get()", e);
        }
    }

    
    /**
     * Retrieves an object from the database by a UUID column.
     *
     * @param clazz  The class of the object to retrieve.
     * @param uuidCol The name of the UUID column.
     * @param uuid   The UUID value.
     * @param <T>    The type of the object.
     * @return The retrieved object, or null if not found.
     * @throws IllegalStateException if the session is closed.
     * @throws RuntimeException      if a database error occurs.
     */
    public <T> T get(Class<T> clazz, String uuidCol, UUID uuid) {
    	if(isClosed) throw new IllegalStateException("Session is closed.");
        try {
            String tableName = getTableName(clazz);

            String sql = "SELECT * FROM `" + tableName + "` WHERE `" + uuidCol + "` = ? LIMIT 1";

            try (PreparedStatement ps = database.newConnection().prepareStatement(sql)) {
                ps.setObject(1, uuid.toString());
                ResultSet rs = ps.executeQuery();

                if (!rs.next()) {
                    return null;
                }

                T instance = clazz.getDeclaredConstructor().newInstance();

                Map<String, Field> columnData = getColumnData(clazz, true);

                for (Map.Entry<String, Field> entry : columnData.entrySet()) {
                    String columnName = entry.getKey();
                    Field field = entry.getValue();
                    field.setAccessible(true);

                    Object value = rs.getObject(columnName);

                    if (field.getType() == UUID.class && value instanceof String) {
                        field.set(instance, UUID.fromString((String) value));
                    } else {
                        field.set(instance, value);
                    }
                }
                persistenceContext.put(getKey(instance, getId(instance)), instance);
                return instance;

            } catch (Exception e) {
                throw new RuntimeException("Failed to retrieve object by UUID", e);
            }

        } catch (Exception e) {
            throw new RuntimeException("Failed to execute get() by UUID", e);
        }
    }
    
    /**
     * Retrieves all objects of a given class from the database.
     *
     * @param clazz The class of the objects to retrieve.
     * @param <T>   The type of the objects.
     * @return A list of all objects of the specified type.
     * @throws IllegalStateException if the session is closed.
     * @throws RuntimeException      if a database error occurs.
     */
    public <T> List<T> getAll(Class<T> clazz){
    	if(isClosed) throw new IllegalStateException("Session is closed.");
    	return executeQuery(clazz, "");
    }

    
    /**
     * Deletes a record from a specified table by its ID.
     *
     * @param tableName The name of the table.
     * @param idCol     The name of the ID column.
     * @param id        The ID of the record to delete.
     * @throws IllegalStateException if the session is closed.
     * @throws RuntimeException      if a database access error occurs.
     */
    public void delete(String tableName, String idCol, long id) {
    	if(isClosed) throw new IllegalStateException("Session is closed.");
        String sql = "DELETE FROM `" + tableName + "` WHERE `" + idCol + "` = ?";

        try (PreparedStatement ps = database.getConn().prepareStatement(sql)) {
            ps.setLong(1, id);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to delete row by ID", e);
        }
    }

    
    /**
     * Closes the database connection and the session.
     */
    public void close() {
    	this.database.disconnect();
    	this.isClosed = true;
    }
    
    /**
     * Checks if the session is closed.
     *
     * @return true if the session is closed, false otherwise.
     */
    public boolean isClosed() {
		return isClosed;
	}
    
    /**
     * Deletes an object from the database.
     *
     * @param o The object to delete.
     * @throws IllegalStateException if the session is closed or no {@code @SQLId} field is found.
     * @throws RuntimeException      if a database access error occurs.
     * @throws IllegalArgumentException if the ID type is unsupported.
     */
    public void delete(Object o) {
    	if(isClosed) throw new IllegalStateException("Session is closed.");
        try {
            Class<?> clazz = o.getClass();
            String tableName = getTableName(clazz);
            Field idField = null;
            String idColumn = null;

            for (Field f : clazz.getDeclaredFields()) {
                if (f.isAnnotationPresent(SQLId.class)) {
                    idField = f;
                    f.setAccessible(true);

                    if (f.isAnnotationPresent(MySQLColumn.class)) {
                        idColumn = f.getAnnotation(MySQLColumn.class).value();
                    } else {
                        idColumn = f.getName();
                    }
                    break;
                }
            }

            if (idField == null) {
                throw new IllegalStateException("No @SQLId field found in " + clazz.getSimpleName());
            }

            Object idValue = idField.get(o);

            if (idValue instanceof Long) {
                delete(tableName, idColumn, (Long) idValue);
            } else if (idValue instanceof UUID) {
                delete(tableName, idColumn, (UUID) idValue);
            } else {
                throw new IllegalArgumentException("Unsupported ID type: " + idValue.getClass());
            }

            String key = getKey(o, getId(o));
            persistenceContext.remove(key);

        } catch (Exception e) {
            throw new RuntimeException("Failed to delete object", e);
        }
    }
    
    /**
     * Retrieves the ID of a record based on a specific column and value.
     *
     * @param clazz  The class of the object.
     * @param column The name of the column to search by.
     * @param value  The value to match.
     * @return The ID of the found record, or -1L if the table does not exist.
     * @throws IllegalAccessException     if the application cannot access the ID field.
     * @throws IllegalClassFormatException if the class has multiple ID fields.
     * @throws SQLException               if a database access error occurs.
     * @throws IllegalStateException      if the session is closed.
     */
    public Long getIdBy(Class<?> clazz, String column, Object value) throws IllegalAccessException, IllegalClassFormatException, SQLException {
    	if(isClosed) throw new IllegalStateException("Session is closed.");
    	Long id = null;
    	String tableName = getTableName(clazz);
    	String idCol = getIdColumn(clazz);
    	
    	try(Connection conn = database.newConnection(); PreparedStatement ps = conn.prepareStatement("SELECT " + idCol + " FROM " + tableName + " WHERE `" + column + "` = ? limit 1")){
    		setupPreparedStatementParams(ps, 1, value);
    		
    		ResultSet rs = ps.executeQuery();
    		if(rs.next()) {
    			id = rs.getLong(idCol);
    		}
    	}catch(SQLException ex) {
    		if(ex.getErrorCode() == 1146) {
    			return -1L;
    		}else {
    			throw new RuntimeException("Database error", ex);
    		}
    	}
    	return id;
    }

    /**
     * Deletes a record from a specified table by its UUID.
     *
     * @param tableName The name of the table.
     * @param uuidCol   The name of the UUID column.
     * @param uuid      The UUID value of the record to delete.
     * @throws IllegalStateException if the session is closed.
     * @throws RuntimeException      if a database access error occurs.
     */
    public void delete(String tableName, String uuidCol, UUID uuid) {
    	if(isClosed) throw new IllegalStateException("Session is closed.");
        String sql = "DELETE FROM `" + tableName + "` WHERE `" + uuidCol + "` = ?";

        try (PreparedStatement ps = database.getConn().prepareStatement(sql)) {
            ps.setString(1, uuid.toString()); // If UUID is stored as CHAR(36)
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to delete row by UUID", e);
        }
    }

    
    /**
     * Creates a new table in the database based on a class definition.
     * <p>
     * This method inspects the annotations on the class fields to determine
     * column names, types, and constraints (e.g., primary keys, foreign keys, not null).
     * It will create the table only if it does not already exist.
     * </p>
     *
     * @param tableName The name of the table to create.
     * @param clazz     The class representing the table structure.
     * @throws IllegalStateException if the session is closed.
     * @throws RuntimeException      if the class has no {@code @MySQLField} or {@code @SQLId} annotation,
     * or if the class has multiple {@code @SQLId} columns.
     */
    public void createTable(String tableName, Class<?> clazz) {
    	if(isClosed) throw new IllegalStateException("Session is closed.");
        Map<String, Field> colsData = new LinkedHashMap<>();
        Map<String, Boolean> notNullConstraints = new HashMap<>();
        String idColumn = null;
        List<String> primaryKeyColumns = new ArrayList<>();
        List<String> foreignKeyDefs = new ArrayList<>();

        boolean isAutoMapped = isAutoMapped(clazz);

        for (Field f : clazz.getDeclaredFields()) {
            String columnName;
            boolean isNotNull = false;

            if (f.isAnnotationPresent(MySQLColumn.class)) {
                MySQLColumn msC = f.getAnnotation(MySQLColumn.class);
                columnName = msC.value();
                isNotNull = msC.isNotNull();
            } else if (isAutoMapped) {
                columnName = f.getName();
            } else {
                continue;
            }

            colsData.put(columnName, f);
            notNullConstraints.put(columnName, isNotNull);

            if (f.isAnnotationPresent(SQLId.class)) {
                if (idColumn != null) {
                    throw new RuntimeException("Table " + tableName + " can only have one SQLId column!");
                }
                idColumn = columnName;
            }

            if (f.isAnnotationPresent(SQLPrimaryKey.class)) {
                primaryKeyColumns.add(columnName);
            }

            if (f.isAnnotationPresent(SQLForeignKey.class)) {
                SQLForeignKey fk = f.getAnnotation(SQLForeignKey.class);
                String fkDef = String.format("FOREIGN KEY (`%s`) REFERENCES `%s`(`%s`)",
                        columnName, fk.table(), fk.attribute());
                foreignKeyDefs.add(fkDef);
            }
        }

        if (idColumn == null) {
            throw new RuntimeException("Table " + tableName + " must have one SQLId column!");
        }

        StringBuilder codeBuilder = new StringBuilder("CREATE TABLE IF NOT EXISTS `")
                .append(tableName).append("` (\n");

        List<String> columnDefs = new ArrayList<>();

        for (Map.Entry<String, Field> entry : colsData.entrySet()) {
            String colName = entry.getKey();
            Field field = entry.getValue();
            Class<?> type = field.getType();
            String sqlType = mapJavaTypeToMySQL(type);

            StringBuilder colDef = new StringBuilder("  `").append(colName).append("` ").append(sqlType);

            if (colName.equals(idColumn)) {
                colDef.append(" AUTO_INCREMENT UNIQUE NOT NULL");
                primaryKeyColumns.add(idColumn);
            } else if (notNullConstraints.getOrDefault(colName, false) || primaryKeyColumns.contains(colName)) {
                colDef.append(" NOT NULL");
            }

            columnDefs.add(colDef.toString());
        }

        if (!primaryKeyColumns.isEmpty()) {
            columnDefs.add("PRIMARY KEY (" + String.join(", ", primaryKeyColumns) + ")");
        }

        columnDefs.addAll(foreignKeyDefs);

        codeBuilder.append(String.join(",\n", columnDefs));
        codeBuilder.append("\n);");

        String sql = codeBuilder.toString();

        try (Connection conn = this.database.newConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Executes a custom SQL query and maps the results to a list of objects.
     * <p>
     * The query should be a valid SQL WHERE or ORDER BY clause.
     * For example: "WHERE status = 'active' ORDER BY created_at DESC"
     * </p>
     *
     * @param clazz The class to which the query results will be mapped.
     * @param query The SQL query fragment (e.g., WHERE clause).
     * @param <T>   The type of the objects in the result list.
     * @return A list of objects from the query results.
     * @throws IllegalStateException if the session is closed.
     * @throws RuntimeException      if a database access error or mapping error occurs.
     */
    public <T> List<T> executeQuery(Class<T> clazz, String query) {
    	if(isClosed) throw new IllegalStateException("Session is closed.");
        List<T> results = new ArrayList<>();


        String tableName = getTableName(clazz);
        try (PreparedStatement ps = database.newConnection().prepareStatement("SELECT * FROM " + tableName + " obj " + query);
             ResultSet rs = ps.executeQuery()) {

            Map<String, Field> columnData = getColumnData(clazz, true);

            while (rs.next()) {
                T instance = clazz.getDeclaredConstructor().newInstance();

                for (Map.Entry<String, Field> entry : columnData.entrySet()) {
                    String columnName = entry.getKey();
                    Field field = entry.getValue();
                    field.setAccessible(true);

                    Object value = rs.getObject(columnName);

                    if (field.getType() == UUID.class && value instanceof String) {
                        field.set(instance, UUID.fromString((String) value));
                    } else {
                        field.set(instance, value);
                    }
                }
                
                Long id = getId(instance);
                String key = getKey(instance, id);
                persistenceContext.put(key, instance); 
                results.add(instance);
            }

        } catch (Exception e) {
            throw new RuntimeException("Failed to execute query for class " + clazz.getSimpleName(), e);
        }

        return results;
    }

    
    /**
     * Executes a native SQL query and returns the results as a list of object arrays.
     *
     * @param query The native SQL query to execute.
     * @return A list of object arrays, where each array represents a row from the result set.
     * @throws IllegalStateException if the session is closed.
     * @throws RuntimeException      if a database access error occurs.
     */
    public List<Object[]> executeNativeQuery(String query) {
    	if(isClosed) throw new IllegalStateException("Session is closed.");
        List<Object[]> results = new ArrayList<>();

        try (PreparedStatement ps = database.newConnection().prepareStatement(query);
             ResultSet rs = ps.executeQuery()) {

            int columnCount = rs.getMetaData().getColumnCount();

            while (rs.next()) {
                Object[] row = new Object[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    row[i] = rs.getObject(i + 1);
                }
                results.add(row);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to execute native query", e);
        }

        return results;
    }

    private static String mapJavaTypeToMySQL(Class<?> type) {
        if (type == int.class || type == Integer.class) return "INT";
        if (type == long.class || type == Long.class) return "BIGINT";
        if (type == short.class || type == Short.class) return "SMALLINT";
        if (type == byte.class || type == Byte.class) return "TINYINT";
        if (type == boolean.class || type == Boolean.class) return "BOOLEAN";
        if (type == float.class || type == Float.class) return "FLOAT";
        if (type == double.class || type == Double.class) return "DOUBLE";
        if (type == char.class || type == Character.class) return "CHAR(1)";
        if (type == String.class) return "VARCHAR(255)";
        if (type == UUID.class) return "VARCHAR(36)";
        if (type == java.util.Date.class || type == java.sql.Timestamp.class) return "DATETIME";
        if (type == java.sql.Date.class) return "DATE";
        if (type == java.sql.Time.class) return "TIME";
        if (type == byte[].class) return "BLOB";

        return "TEXT";
    }

    private static String getKey(Object object, long id){
        Class<?> clazz = object.getClass();
        String key = clazz.getName() + "#" + id;
        return key;
    }

    private static Long getId(Object o) throws IllegalAccessException, IllegalClassFormatException {
        Class<?> clazz = o.getClass();
        Long id = null;
        boolean b = false;
        for(Field field : clazz.getDeclaredFields()){
            if(b){
                throw new IllegalClassFormatException("Multiple ID field");
            }
            if(field.isAnnotationPresent(SQLId.class)){
                field.setAccessible(true);
                id = (Long) field.get(o);
            }
        }
        return id;
    }
    /**
     * Updates an existing object in the database.
     * <p>
     * The object must have a valid ID. Only the fields marked for mapping will be updated.
     * </p>
     *
     * @param o The object to update.
     * @throws SQLException               if a database access error occurs.
     * @throws IllegalClassFormatException if the class has multiple ID fields.
     * @throws IllegalAccessException     if the application cannot access the fields.
     * @throws IllegalStateException      if the session is closed.
     */
    public void update(Object o) throws SQLException, IllegalClassFormatException, IllegalAccessException {
    	if(isClosed) throw new IllegalStateException("Session is closed.");
        String tableName = getTableName(o);
        Map<String, Field> params = getColumnData(o, false);
        String idCol = getIdColumn(o);
        Long id = getId(o);

        StringBuilder codeBuilder = new StringBuilder("UPDATE ");
        codeBuilder.append(tableName).append(" SET ");

        List<String> paramKeys = new ArrayList<>(params.keySet());
        for (int i = 0; i < paramKeys.size(); i++) {
            codeBuilder.append(paramKeys.get(i)).append(" = ?");
            if (i < paramKeys.size() - 1) {
                codeBuilder.append(", ");
            }
        }
        codeBuilder.append(" WHERE ").append(idCol).append(" = ?");

        try (PreparedStatement ps = database.getConn().prepareStatement(codeBuilder.toString())) {
            int i = 1;
            for (String key : paramKeys) {
                Field field = params.get(key);
                field.setAccessible(true);
                Object value = field.get(o);
                setupPreparedStatementParams(ps, i, value);                
                i++;
            }
            ps.setLong(i, id);
            ps.executeUpdate();
        }
    }
    
    private static void setupPreparedStatementParams(PreparedStatement ps, int i, Object value) throws SQLException {
    	if (value == null) {
            ps.setObject(i, null);
        } else if (value instanceof Integer) {
            ps.setInt(i, (Integer) value);
        } else if (value instanceof String) {
            ps.setString(i, (String) value);
        } else if (value instanceof Short) {
            ps.setShort(i, (Short) value);
        } else if (value instanceof Float) {
            ps.setFloat(i, (Float) value);
        } else if (value instanceof Double) {
            ps.setDouble(i, (Double) value);
        } else if (value instanceof Long) {
            ps.setLong(i, (Long) value);
        } else if (value instanceof Boolean) {
            ps.setBoolean(i, (Boolean) value);
        } else if (value instanceof Byte) {
            ps.setByte(i, (Byte) value);
        } else if (value instanceof Character) {
            ps.setString(i, value.toString());
        } else if (value instanceof java.math.BigDecimal) {
            ps.setBigDecimal(i, (java.math.BigDecimal) value);
        } else if (value instanceof java.sql.Date) {
            ps.setDate(i, (java.sql.Date) value);
        } else if (value instanceof java.sql.Time) {
            ps.setTime(i, (java.sql.Time) value);
        } else if (value instanceof java.sql.Timestamp) {
            ps.setTimestamp(i, (java.sql.Timestamp) value);
        } else if (value instanceof java.util.Date) {
            ps.setTimestamp(i, new java.sql.Timestamp(((java.util.Date) value).getTime()));
        } else if (value instanceof java.time.LocalDate) {
            ps.setDate(i, java.sql.Date.valueOf((java.time.LocalDate) value));
        } else if (value instanceof java.time.LocalDateTime) {
            ps.setTimestamp(i, java.sql.Timestamp.valueOf((java.time.LocalDateTime) value));
        } else if (value instanceof byte[]) {
            ps.setBytes(i, (byte[]) value);
        } else {
            ps.setString(i, value.toString());
        }
    }


    private static String getTableName(Object o){
        Class<?> clazz = o.getClass();
        if(!clazz.isAnnotationPresent(MySQLField.class)) throw new RuntimeException("No such annotation declared for class " + clazz.getName());
        MySQLField msF = clazz.getAnnotation(MySQLField.class);
        return msF.value();
    }
    
    private static String getTableName(Class<?> clazz){
        if(!clazz.isAnnotationPresent(MySQLField.class)) throw new RuntimeException("No such annotation declared for class " + clazz.getName());
        MySQLField msF = clazz.getAnnotation(MySQLField.class);
        return msF.value();
    }

    private static boolean isAutoMapped(Object o){
        Class<?> clazz = o.getClass();
        if(!clazz.isAnnotationPresent(MySQLField.class)) throw new RuntimeException("No such annotation declared for class " + clazz.getName());
        MySQLField msF = clazz.getAnnotation(MySQLField.class);
        return msF.autoMapped();
    }

    private static boolean isAutoMapped(Class<?> clazz){
        if(!clazz.isAnnotationPresent(MySQLField.class)) throw new RuntimeException("No such annotation declared for class " + clazz.getName());
        MySQLField msF = clazz.getAnnotation(MySQLField.class);
        return msF.autoMapped();
    }

    private static Map<String, Field> getColumnData(Object o, boolean includeId){
        Map<String, Field> colsData = new LinkedHashMap<>();
        Class<?> clazz = o.getClass();
        boolean isAutoMapped = isAutoMapped(o);
        for(Field f : clazz.getDeclaredFields()){
            if(f.isAnnotationPresent(MySQLColumn.class)){
                if(f.isAnnotationPresent(SQLId.class) && !includeId) continue;
                MySQLColumn msC = f.getAnnotation(MySQLColumn.class);
                colsData.put(msC.value(), f);
            }else if(f.isAnnotationPresent(SQLId.class) && includeId){
                colsData.put(f.getName(), f);
            }else if(isAutoMapped){
                colsData.put(f.getName(), f);
            }
        }
        return colsData;
    }
    
    private static Map<String, Field> getColumnData(Class<?> clazz, boolean includeId){
        Map<String, Field> colsData = new LinkedHashMap<>();
        boolean isAutoMapped = isAutoMapped(clazz);
        for(Field f : clazz.getDeclaredFields()){
            if(f.isAnnotationPresent(MySQLColumn.class)){
                if(f.isAnnotationPresent(SQLId.class) && !includeId) continue;
                MySQLColumn msC = f.getAnnotation(MySQLColumn.class);
                colsData.put(msC.value(), f);
            }else if(f.isAnnotationPresent(SQLId.class) && includeId){
                colsData.put(f.getName(), f);
            }else if(isAutoMapped){
                colsData.put(f.getName(), f);
            }
        }
        return colsData;
    }

    private static String getIdColumn(Object o)throws IllegalAccessException, IllegalClassFormatException {
        Class<?> clazz = o.getClass();
        String id = null;
        boolean b = false;
        for(Field field : clazz.getDeclaredFields()){
            if(b){
                throw new IllegalClassFormatException("Multiple ID field");
            }
            if(field.isAnnotationPresent(SQLId.class)){
                if(field.isAnnotationPresent(MySQLColumn.class)) {
                	MySQLColumn msC = field.getAnnotation(MySQLColumn.class);
                	id = msC.value();
                }else {
                	id = field.getName();
                }
            }
        }
        return id;
    }
    
    private static String getIdColumn(Class<?> clazz) throws IllegalAccessException, IllegalClassFormatException {
        String id = null;
        boolean b = false;
        for(Field field : clazz.getDeclaredFields()){
            if(b){
                throw new IllegalClassFormatException("Multiple ID field");
            }
            if(field.isAnnotationPresent(SQLId.class)){
                if(field.isAnnotationPresent(MySQLColumn.class)) {
                	MySQLColumn msC = field.getAnnotation(MySQLColumn.class);
                	id = msC.value();
                }else {
                	id = field.getName();
                }
            }
        }
        return id;
    }
}
