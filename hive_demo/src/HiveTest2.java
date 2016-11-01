import sun.java2d.pipe.SpanIterator;

import java.sql.*;

/**
 * Created by Mark Tao on 2016/11/1 21:26.
 */


public class HiveTest2 {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        Connection con = DriverManager.getConnection("jdbc:hive2://1.1.1.2:10000/default", "root", "");
        Statement statement = con.createStatement();

        String sql = "select * from people";
        ResultSet resultSet = statement.executeQuery(sql);

        if (resultSet.next()) {
            System.out.println(resultSet.getString(2));
        }

    }
}
