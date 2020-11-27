package ua.kiev.prog.case2;

import ua.kiev.prog.shared.ColumnName;
import ua.kiev.prog.shared.Id;
import ua.kiev.prog.shared.Length;

import java.lang.reflect.Field;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractDAO<K, T> {
    private final Connection conn;
    private final String table;

    public AbstractDAO(Connection conn, String table) {
        this.conn = conn;
        this.table = table;
    }

    public void add(T t) {
        try {
            Field[] fields = t.getClass().getDeclaredFields();

            StringBuilder names = new StringBuilder();
            StringBuilder values = new StringBuilder();

            for (Field f : fields) {
                f.setAccessible(true);

                names.append(f.getName()).append(',');
                values.append('"').append(f.get(t)).append("\",");
            }
            names.deleteCharAt(names.length() - 1); // last ','
            values.deleteCharAt(values.length() - 1); // last ','

            String sql = "INSERT INTO " + table + "(" + names.toString() +
                    ") VALUES(" + values.toString() + ")";

            try (Statement st = conn.createStatement()) {
                st.execute(sql);
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public void update(T t) {
        try {
            Field[] fields = t.getClass().getDeclaredFields();
            Field id = null;

            for (Field f : fields) {
                if (f.isAnnotationPresent(Id.class)) {
                    id = f;
                    id.setAccessible(true);
                    break;
                }
            }
            if (id == null)
                throw new RuntimeException("No Id field");

            StringBuilder sb = new StringBuilder();

            for (Field f : fields) {
                if (f != id) {
                    f.setAccessible(true);

                    sb.append(f.getName())
                            .append(" = ")
                            .append('"')
                            .append(f.get(t))
                            .append('"')
                            .append(',');
                }
            }

            sb.deleteCharAt(sb.length() - 1);

            String sql = "UPDATE " + table + " SET " + sb.toString() + " WHERE " +
                    id.getName() + " = \"" + id.get(t) + "\"";

            try (Statement st = conn.createStatement()) {
                st.execute(sql);
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public void delete(T t) {
        try {
            Field[] fields = t.getClass().getDeclaredFields();
            Field id = null;

            for (Field f : fields) {
                if (f.isAnnotationPresent(Id.class)) {
                    id = f;
                    id.setAccessible(true);
                    break;
                }
            }
            if (id == null)
                throw new RuntimeException("No Id field");

            String sql = "DELETE FROM " + table + " WHERE " + id.getName() +
                    " = \"" + id.get(t) + "\"";

            try (Statement st = conn.createStatement()) {
                st.execute(sql);
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public List<T> getAll(Class<T> cls) {
        List<T> res = new ArrayList<>();

        try {
            try (Statement st = conn.createStatement()) {
                try (ResultSet rs = st.executeQuery("SELECT * FROM " + table)) {
                    ResultSetMetaData md = rs.getMetaData();

                    while (rs.next()) {
                        T t = cls.newInstance();

                        for (int i = 1; i <= md.getColumnCount(); i++) {
                            String columnName = md.getColumnName(i);

                            Field field = cls.getDeclaredField(columnName);
                            field.setAccessible(true);

                            field.set(t, rs.getObject(columnName));
                        }

                        res.add(t);
                    }
                }
            }

            return res;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public List<T> getAllEx(Class<T> cls, String... names) {
        List<T> res = new ArrayList<>();

        try {
            try (Statement st = conn.createStatement()) {
                try (ResultSet rs = st.executeQuery("SELECT * FROM " + table)) {
                    while (rs.next()) {
                        T t = cls.newInstance();

                        for (String name : names) {
                            Field field = cls.getDeclaredField(name);
                            field.setAccessible(true);

                            field.set(t, rs.getObject(name));
                        }

                        res.add(t);
                    }
                }
            }

            return res;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public void init(Class<T> cls) {
        try (Statement st = conn.createStatement()) {
            StringBuilder sb = new StringBuilder();
            sb.append("CREATE TABLE ").append(cls.getSimpleName()).append("(");
            Field[] fields = cls.getDeclaredFields();
            for (Field f : fields) {
                f.setAccessible(true);
                if(f.getAnnotation(ColumnName.class) != null){
                    sb.append(f.getAnnotation(ColumnName.class).name()).append(" ");
                } else {
                    sb.append(f.getName());
                }

                if (f.getType().equals(String.class)) {
                    sb.append("VARCHAR");
                    if (f.isAnnotationPresent(Length.class)) {
                        Length length = f.getAnnotation(Length.class);
                        sb.append("(" + length.length() + ")");
                    }

                } else if (f.getType().equals(int.class)) {
                    sb.append("INT");
                }
                sb.append(" ");
                sb.append("NOT NULL");
                if (f.isAnnotationPresent(Id.class)) {
                    sb.append(" AUTO_INCREMENT PRIMARY KEY");
                }
                sb.append(",");
            }

            sb.deleteCharAt(sb.length() - 1);
            sb.append(")");

            System.out.println(sb.toString());

            st.execute("DROP TABLE IF EXISTS " + cls.getSimpleName());
            st.execute(sb.toString());

        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }
}
