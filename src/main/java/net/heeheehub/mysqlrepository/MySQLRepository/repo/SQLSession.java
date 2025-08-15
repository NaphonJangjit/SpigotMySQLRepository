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

public class SQLSession {

    private Database database;
    private Map<String, Object> persistenceContext;
    private SQLTransaction tx;
    public SQLSession(Database database) throws SQLException {
        this.database = new Database(database.getHost(), database.getPort(), database.getDbName(), database.getUser(), database.getPassword());
        this.database.connect();
        this.persistenceContext = new HashMap<>();
        this.tx = new SQLTransaction(database);
    }
    
    public SQLTransaction beginTransaction() throws SQLException {
    	tx.begin();
    	return tx;
    }
    
    public void endTransaction() throws SQLException {
    	tx.end();
    }

    public Long persists(Object object) throws SQLException, IllegalClassFormatException, IllegalAccessException {
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

    public <T> T get(Class<T> clazz, Long id) {
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

    public <T> T get(Class<T> clazz, String uuidCol, UUID uuid) {
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
    
    public <T> List<T> getAll(Class<T> clazz){
    	return executeQuery(clazz, "");
    }

    public void delete(String tableName, String idCol, long id) {
        String sql = "DELETE FROM `" + tableName + "` WHERE `" + idCol + "` = ?";

        try (PreparedStatement ps = database.getConn().prepareStatement(sql)) {
            ps.setLong(1, id);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to delete row by ID", e);
        }
    }

    public void delete(Object o) {
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
    
    public Long getIdBy(Class<?> clazz, String column, Object value) throws IllegalAccessException, IllegalClassFormatException, SQLException {
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


    public void delete(String tableName, String uuidCol, UUID uuid) {
        String sql = "DELETE FROM `" + tableName + "` WHERE `" + uuidCol + "` = ?";

        try (PreparedStatement ps = database.getConn().prepareStatement(sql)) {
            ps.setString(1, uuid.toString()); // If UUID is stored as CHAR(36)
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to delete row by UUID", e);
        }
    }

    public void createTable(String tableName, Class<?> clazz) {
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

            // Auto-increment and NOT NULL for SQLId
            if (colName.equals(idColumn)) {
                colDef.append(" PRIMARY KEY AUTO_INCREMENT UNIQUE NOT NULL");
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


    public <T> List<T> executeQuery(Class<T> clazz, String query) {
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

    public List<Object[]> executeNativeQuery(String query) {

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

    public void update(Object o) throws SQLException, IllegalClassFormatException, IllegalAccessException {
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
