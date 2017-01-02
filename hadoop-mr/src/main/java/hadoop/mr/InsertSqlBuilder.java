package hadoop.mr;

import org.apache.commons.collections.map.LinkedMap;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * Created by yong on 2016/12/29.
 */
public class InsertSqlBuilder {
    private StringBuffer columns = new StringBuffer();
    private StringBuffer values = new StringBuffer();
    private String table;

    public InsertSqlBuilder(String table) {
        this.table = table;
    }

    public InsertSqlBuilder value(String key, Object val) {
        this.columns.append(key).append(" ,");
        if (val instanceof Number) {
            this.values.append(val).append(" ,");
        } else if (val instanceof String) {
            this.values.append(String.format("'%s'", val)).append(" ,");
        } else if (val instanceof Date) {
            String dareStr = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(val);
            this.values.append(String.format("'%s'", dareStr)).append(" ,");
        } else {
            this.values.append(val).append(" ,");
        }
        return this;
    }

    public String toSql() {
        StringBuffer sb = new StringBuffer("insert into ").append(this.table);
        sb.append(" (").append(this.columns.toString().substring(0, this.columns.length() - 1)).append(")");
        sb.append(" values");
        sb.append(" (").append(this.values.toString().substring(0, this.values.length() - 1)).append(")");
        return sb.toString();
    }

    public static void main(String[] args) {
        InsertSqlBuilder ib = new InsertSqlBuilder("tab_test");
        Map<String, Object> map = new LinkedMap();

        map.put("s1", 1);
        map.put("s1-1", 1.1);
        map.put("s2", new Date());
        map.put("s2-1", null);
        map.put("s3", "v3");

        for (String key : map.keySet()) {
            ib.value(key, map.get(key));
        }

        System.out.println(ib.toSql());

    }
}
