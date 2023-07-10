import com.vincent.common.GmallConfig;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class CheckTableTest {
    public static void main(String[] args) throws SQLException {
        System.out.println(checkTable("xsh", "id,aaa,bbb", null, null));
    }

    private static String checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) throws SQLException {
        // 封装建表 SQL
        StringBuilder sql = new StringBuilder();
        sql.append("create table if not exists " + GmallConfig.HBASE_SCHEMA
                + "." + sinkTable + "(\n");
        String[] columnArr = sinkColumns.split(",");
        // 为主键及扩展字段赋默认值
        if (sinkPk == null) {
            sinkPk = "id";
        }
        if (sinkExtend == null) {
            sinkExtend = "";
        }
        // 遍历添加字段信息
        for (int i = 0; i < columnArr.length; i++) {
            sql.append(columnArr[i] + " varchar");
            // 判断当前字段是否为主键
            if (sinkPk.equals(columnArr[i])) {
                sql.append(" primary key");
            }
            // 如果当前字段不是最后一个字段，则追加","
            if (i < columnArr.length - 1) {
                sql.append(",\n");
            }
        }
        sql.append(")");
        sql.append(sinkExtend);
        String createStatement = sql.toString();
        return createStatement;
    }
}
