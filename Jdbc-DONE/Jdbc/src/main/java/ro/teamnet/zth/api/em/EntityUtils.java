package ro.teamnet.zth.api.em;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import ro.teamnet.zth.api.annotations.Column;
import ro.teamnet.zth.api.annotations.Id;
import ro.teamnet.zth.api.annotations.Table;

/*
* Clasa contine metode ajutatoare pentru colectarea
* informatiilor despre campuri si clase dinamic, la runtime
*/
public class EntityUtils {

    public static final String EMPTY_STRING = "";

    private EntityUtils() {
        throw new UnsupportedOperationException();
    }

    /*
    * Returneaza numele tabelei asociate clasei entity
    */
    public static String getTableName(Class entity) {
        Table tableAnnotation = (Table) entity.getAnnotation(Table.class);
        return EMPTY_STRING.equals(tableAnnotation.name()) ? entity.getClass().getSimpleName() : tableAnnotation.name();
    }

    /*
    * Returneaza o lista (List<ColumnInfo>) cu
    * informatii despre campurile clasei entity
    */
    public static List<ColumnInfo> getColumns(Class entity) {
        List<ColumnInfo> columns = new ArrayList<>();
        Field[] fields = entity.getDeclaredFields();
        for (Field field : fields) {
            Column column = field.getAnnotation(Column.class);
            ColumnInfo columnInfo = new ColumnInfo();
            columnInfo.setColumnName(field.getName());
            columnInfo.setColumnType(field.getType());
            if (column != null) {
                columnInfo.setDbName(column.name());
            } else {
                Id id = field.getAnnotation(Id.class);
                columnInfo.setDbName(id.name());
                columnInfo.setId(true);
            }
            columns.add(columnInfo);
        }
        return columns;
    }

    /*
    * Metoda primeste un obiect din baza de date (value),
    * si il transforma intr-un obiect de tip wantedType
    */
    public static Object castFromSqlType(Object value, Class wantedType) {
        if (value != null) {
            if (value instanceof BigDecimal) {
                BigDecimal bdValue = (BigDecimal) value;
                return wantedType.equals(Integer.class) ? bdValue.intValue() :
                        wantedType.equals(Long.class) ? bdValue.longValue() :
                                wantedType.equals(Float.class) ? bdValue.floatValue() :
                                        wantedType.equals(Double.class) ? bdValue.doubleValue() : value;
            } else {
                return value;
            }
        }
        return null;
    }

    /*
    * Returneaza o lista (List<Field>) ce contine campurile
    * clasei clazz annotate cu annotarea annotation
    */
    public static List<Field> getFieldsByAnnotations(Class clazz, Class annotation) {
        List<Field> fields = new ArrayList<>();
        Field[] declaredFields = clazz.getDeclaredFields();
        for (Field declaredField : declaredFields) {
            if (declaredField.getAnnotation(annotation) != null) {
                fields.add(declaredField);
            }
        }
        return fields;
    }

    public static Object getSqlValue(Object object) throws IllegalAccessException {
        if (object == null) {
            return null;
        }
        if (object.getClass().getAnnotation(Table.class) != null) {
            Field idField = getFieldsByAnnotations(object.getClass(), Id.class).get(0);
            idField.setAccessible(true);
            return idField.get(object);
        } else {
            return object;
        }
    }
}
