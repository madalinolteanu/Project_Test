package ro.teamnet.zth.api.em;

import java.sql.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/*
* Clasa ce contine metode ajutatoare crearii de interogari SQL
* de tip select/insert/delete/update in mod dinamic. Pentru aceasta
* sunt declarate si folosite campurile:
 *      tableName - numele tabelei pe care se executa interogarile
 *      queryColumns - coloanele implicate in interogari
 *      queryType - tipul interogarii SQL: select/insert/update/delete
 *      conditions - conditiile interogarii
*/
public class QueryBuilder {

    private Object tableName;
    private List<ColumnInfo> queryColumns;
    private QueryType queryType;
    private List<Condition> conditions;

    /**
     * Adauga o conditie la conditiile deja existente in interogare
     */
    public QueryBuilder addCondition(Condition condition) {
        if (this.conditions == null){
            this.conditions = new ArrayList<>();
        }
        this.conditions.add(condition);
        return this;
    }

    public QueryBuilder setTableName(Object tableName) {
        this.tableName = tableName;
        return this;
    }

    /**
     * Adauga o coloana la coloanele deja implicate in interogare
     */
    public QueryBuilder addQueryColumns(List<ColumnInfo> queryColumns) {
        if (this.queryColumns == null){
            this.queryColumns = new ArrayList<>();
        }
        this.queryColumns.addAll(queryColumns);
        return this;
    }

    public QueryBuilder setQueryType(QueryType queryType) {
        this.queryType = queryType;
        return this;
    }

    /*
    * Metoda ce creaza interogari de tip select/update/delete/insert,
    * in functie de valoarea lui queryType
    */
    public String createQuery() {
        if (QueryType.SELECT.equals(this.queryType)){
            return createSelectQuery();
        } else if (QueryType.INSERT.equals(this.queryType)) {
            return createInsertQuery();
        } else if (QueryType.UPDATE.equals(this.queryType)) {
            return createUpdateQuery();
        } else if (QueryType.DELETE.equals(this.queryType)) {
            return createDeleteQuery();
        }
        return null;
    }

    /*
    * Metoda ce creaza o interogare de tip select, folosind
    * coloanele din queryColumns si conditiile din conditions
    */
    private String createSelectQuery() {
        StringBuilder sql = new StringBuilder();
        sql.append("select ");
        boolean isFirst = true;
        for(ColumnInfo columnInfo : queryColumns) {
            if(!isFirst) {
                sql.append(",");
            }
            sql.append(tableName + "." + columnInfo.getDbName());
            isFirst = false;
        }
        sql.append(" from " + tableName);

        boolean whereAdded = false;
        if(conditions != null && !conditions.isEmpty()) {
            for(Condition condition : conditions) {
                sql.append(whereAdded ? " and" : " where ").append(condition.getColumnName()).append("=")
                    .append(getValueForQuery(condition.getValue()));
                whereAdded = true;
            }
        }
        return sql.toString();
    }

    /*
    * Metoda ce creaza o interogare de tip delete, folosind
    * conditiile din conditions
    */
    private String createDeleteQuery() {
        StringBuilder sql = new StringBuilder();
        sql.append("delete from ").append(tableName);
        boolean whereAdded = false;
        if (conditions != null  && !conditions.isEmpty()){
            for (Condition condition : conditions) {
                sql.append(whereAdded ? " and" : " where ").append(condition.getColumnName()).append("=").append(getValueForQuery(condition.getValue()));
                whereAdded = true;
            }
        }
        return sql.toString();
    }

    /*
    * Metoda ce creaza o interogare de tip update, folosind
    * coloanele din queryColumns si conditiile din conditions
    */
    private String createUpdateQuery() {
        StringBuilder sql = new StringBuilder();
        sql.append("update ").append(tableName).append(" set ");
        boolean first = true;
        for (ColumnInfo column : queryColumns) {
            if (!column.isId()) {
                if (!first) {
                    sql.append(",");
                } else {
                    first = false;
                }
                sql.append(column.getDbName()).append("=").append(getValueForQuery(column.getValue()));
            }
        }

        boolean whereAdded = false;
        if (conditions != null  && !conditions.isEmpty()){
            for (Condition condition : conditions) {
                sql.append(whereAdded ? " and" : " where ").append(condition.getColumnName()).append("=").append(getValueForQuery(condition.getValue()));
                whereAdded = true;
            }
        }
        return sql.toString();
    }

    /*
    * Metoda ce creaza o interogare de tip insert, folosind
    * coloanele si valorile acestora din queryColumns
    */
    private String createInsertQuery() {
        StringBuilder sql = new StringBuilder();
        sql.append("insert into ").append(tableName).append(" (");
        StringBuilder sqlValues = new StringBuilder(" values (");
        boolean first = true;
        for (ColumnInfo columnInfo : queryColumns) {
            /*if (columnInfo.isId()) {
                sql.append(columnInfo.getValue());
            }*/
            if (!first) {
                sql.append(",");
                sqlValues.append(",");
            } else {
                first = false;
            }
            sql.append(columnInfo.getDbName());
            sqlValues.append(getValueForQuery(columnInfo.getValue()));
        }

        sql.append(") ");
        sqlValues.append(")");
        sql.append(sqlValues);

        return sql.toString();
    }

    /*
    * Metoda transforma obiectul value, daca este de tip Date/String,
    * intr-un format potrivit standardului SQL
    */
    private String getValueForQuery(Object value) {
        if (value == null){
            return null;
        }
        if (value instanceof String){
            return "'" + value + "'";
        } else if (value instanceof java.sql.Date){
            DateFormat dateFormat = new SimpleDateFormat("mm/dd/yyyy");
            return "TO_DATE('"+dateFormat.format((Date)value)+"','mm-dd-YYYY'";
        } else {
            return value.toString();
        }
    }
}
