package ro.teamnet.zth.api.em;

import ro.teamnet.zth.api.annotations.Id;
import ro.teamnet.zth.api.annotations.Table;
import ro.teamnet.zth.api.database.DBManager;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static ro.teamnet.zth.api.em.EntityUtils.castFromSqlType;
import static ro.teamnet.zth.api.em.EntityUtils.getColumns;
import static ro.teamnet.zth.api.em.EntityUtils.getTableName;

/**
 * Created by user on 7/8/2016.
 */
public class EntityManagerImpl implements EntityManager {
    Connection connection = DBManager.getConnection();

    public EntityManagerImpl() throws SQLException, ClassNotFoundException {
    }


    @Override
    public <T> T findById(Class<T> entityClass, int id) {
        try (Connection conn = DBManager.getConnection();
             Statement stmt = conn.createStatement()) {
            QueryBuilder query = new QueryBuilder();
            String tableName = EntityUtils.getTableName(entityClass);
            List<ColumnInfo> columns = EntityUtils.getColumns(entityClass);
            List<Field> fieldsByAnnotations = EntityUtils.getFieldsByAnnotations(entityClass, Id.class);
            Condition condition = new Condition(fieldsByAnnotations.get(0).getAnnotation(Id.class).name(), id);
            query.setTableName(tableName).addQueryColumns(columns).setQueryType(QueryType.SELECT).addCondition(
                    condition);
            String sql = query.createQuery();
            ResultSet rs = stmt.executeQuery(sql);
            T instance = null;
            if (rs.next()) {
                instance = entityClass.newInstance();
                for (ColumnInfo column : columns) {
                    column.setValue(rs.getObject(column.getDbName()));
                    Field field = instance.getClass().getDeclaredField(column.getColumnName());
                    field.setAccessible(true);
                    field.set(instance, EntityUtils.castFromSqlType(column.getValue(), column.getColumnType()));
                }
            }
            return instance;
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }
    }

    @Override
    public int getNextIdVal(String tableName, String columnIdName) throws SQLException, ClassNotFoundException {
        Connection connection = DBManager.getConnection();
        try (Statement statement = connection.createStatement()) {

            StringBuilder query = new StringBuilder();
            query.append("Select max(");
            query.append(columnIdName);
            query.append(") from ");
            query.append(tableName);

            ResultSet resultSet = statement.executeQuery(query.toString());
            resultSet.next();
            Object result = resultSet.getObject(1);
            return ((BigDecimal) result).intValue() + 1;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return 0;
    }

    @Override
    public <T> T insert(T entity) throws NoSuchFieldException, SQLException, ClassNotFoundException {
        String tableName = EntityUtils.getTableName(entity.getClass());
        List<ColumnInfo> columnList = new ArrayList<ColumnInfo>();
        columnList = EntityUtils.getColumns(entity.getClass());
        QueryBuilder query = new QueryBuilder();
        Connection connection = DBManager.getConnection();
        int Id = 0;
        try {

            for (ColumnInfo cinfo : columnList) {
                if (cinfo.isId()) {
                    cinfo.setValue(getNextIdVal(tableName, cinfo.getDbName()));
                    Id = getNextIdVal(tableName, cinfo.getDbName());

                } else {
                    //  entity.getClass().getDeclaredField(cinfo.getColumnName());

                    Field field = entity.getClass().getDeclaredField(cinfo.getColumnName());
                    field.setAccessible(true);
                    cinfo.setValue(field.get(entity));
                }
            }
//            query.s

            query.setTableName(tableName);
            query.addQueryColumns(columnList);
            query.setQueryType(QueryType.INSERT);
            String queryString = query.createQuery();

            System.out.println(queryString);
            Statement statement = connection.createStatement();

            statement.execute(queryString);

            return (T) findById(entity.getClass(), Id);


        } catch (SQLException sqlE) {
            sqlE.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (connection != null)
                connection.close();
        }
        return null;
    }


    @Override
    public <T> List<T> findAll(Class<T> entityClass) throws SQLException {
        //  Connection conn = null;
        Statement stmt = null;
        List<T> list = new ArrayList<T>();
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            connection = DBManager.getConnection();
            stmt = connection.createStatement();
            String table_name = getTableName(entityClass);
            List<ColumnInfo> columnInfo = getColumns(entityClass);
            QueryBuilder queryBuilder = new QueryBuilder();
            queryBuilder.setTableName(table_name);
            queryBuilder.addQueryColumns(columnInfo);
            queryBuilder.setQueryType(QueryType.SELECT);
            String query = queryBuilder.createQuery();
            ResultSet resultSet = stmt.executeQuery(query);
            while (resultSet.next()) {
                T instance = entityClass.newInstance();
                for (ColumnInfo c : columnInfo) {
                    Field f = instance.getClass().getDeclaredField(c.getColumnName());
                    f.setAccessible(true);
                    if (resultSet.getObject(c.getDbName()) instanceof Timestamp) {
                        Date date = new Date((((Timestamp) resultSet.getObject(c.getDbName())).getTime()));
                        f.set(instance, date);
                    } else {
                        f.set(instance, castFromSqlType(resultSet.getObject(c.getDbName()), f.getType()));
                    }
                }
                list.add(instance);
            }
        } catch (SQLException sqlE) {
            sqlE.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (connection != null)
                connection.close();
        }
        return list;
    }

    @Override
    public <T> List<T> findByParams(Class<T> entityClass, Map<String, Object> params) throws NoSuchFieldException {

        List<T> arrayReturned = new ArrayList<>();


        try (Connection conn = DBManager.getConnection();
             Statement stmt = conn.createStatement()) {
            QueryBuilder query = new QueryBuilder();
            String tableName = EntityUtils.getTableName(entityClass);
            List<ColumnInfo> columns = EntityUtils.getColumns(entityClass);
            List<Field> fieldsByAnnotations = EntityUtils.getFieldsByAnnotations(entityClass, Id.class);
            for (Map.Entry<String, Object> entry : params.entrySet()) {
                Condition condition = new Condition(entry.getKey(), entry.getValue());
                query.setTableName(tableName).addQueryColumns(columns).setQueryType(QueryType.SELECT).addCondition(
                        condition);
                String sql = query.createQuery();
                ResultSet rs = stmt.executeQuery(sql);
                T instance = null;
                while (rs.next()) {
                    instance = entityClass.newInstance();
                    for (ColumnInfo column : columns) {
                        column.setValue(rs.getObject(column.getDbName()));
                        Field field = instance.getClass().getDeclaredField(column.getColumnName());
                        field.setAccessible(true);
                        field.set(instance, EntityUtils.castFromSqlType(column.getValue(), column.getColumnType()));
                    }
                    arrayReturned.add(instance);
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }
        return arrayReturned;
    }

    @Override
    public void delete(Object entity) throws SQLException, ClassNotFoundException, NoSuchFieldException, IllegalAccessException {
        try (Connection connection = DBManager.getConnection(); Statement stmt = connection.createStatement()) {
            String tableName = EntityUtils.getTableName(entity.getClass());
            List<ColumnInfo> columns = EntityUtils.getColumns(entity.getClass());
            Condition condition = null;
            for (ColumnInfo col : columns) {
                Field field = entity.getClass().getDeclaredField(col.getColumnName());
                field.setAccessible(true);
                Object value = field.get(entity);
                col.setValue(EntityUtils.getSqlValue(value));
            }
            condition = new Condition(columns.get(0).getDbName(), columns.get(0).getValue());
            QueryBuilder queryBuilder = new QueryBuilder();
            queryBuilder.setTableName(tableName).setQueryType(QueryType.DELETE).addCondition(condition);
            String query = queryBuilder.createQuery();
            System.out.println(query);
            Statement statement = connection.createStatement();
            statement.executeUpdate(query);
        }
    }

    @Override
    public <T> T update(T entity) {
        try {
            Connection connection = DBManager.getConnection();
            String tableName = EntityUtils.getTableName(entity.getClass());
            List<ColumnInfo> columnList = new ArrayList<ColumnInfo>();
            columnList = EntityUtils.getColumns(entity.getClass());
            QueryBuilder query = new QueryBuilder();
            int id = 0;
            Condition condition = null;
            for (ColumnInfo cinfo : columnList) {
                Field field = entity.getClass().getDeclaredField(cinfo.getColumnName());
                field.setAccessible(true);
                cinfo.setValue(field.get(entity));
                if (cinfo.isId()) {
                    id = Integer.parseInt(cinfo.getValue().toString());
                    condition = new Condition(cinfo.getDbName(), id);
                }
            }
            query.setTableName(tableName);
            query.setQueryType(QueryType.UPDATE);
            query.addQueryColumns(columnList);
            query.addCondition(condition);
            String queryString = query.createQuery();
            System.out.println(queryString);
            Statement statement = connection.createStatement();
            statement.executeUpdate(queryString);
            return (T) findById(entity.getClass(), id);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } finally {
            if (connection != null)
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
        }
        return null;
    }
}
