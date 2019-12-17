package sink;

import bean.ActionStat;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

// 自定义 myslq Sink
public class MysqlSink extends
        RichSinkFunction<ActionStat> {

    private static final long serialVersionUID = 1L;
    private Connection connection;
    private PreparedStatement preparedStatement;
    String username = "username";
    String password = "password";
    String dburl = "";
    String drivername = "com.mysql.jdbc.Driver";

    @Override
    public void invoke(ActionStat value) throws Exception {
        Class.forName(drivername);
        connection = DriverManager.getConnection(dburl, username, password);
        String sql = "replace into click_pv(userid,cnt) values(?,?)";
        preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, value.userId);
        preparedStatement.setLong(2, value.count);

        preparedStatement.executeUpdate();
        if (preparedStatement != null) {
            preparedStatement.close();
        }
        if (connection != null) {
            connection.close();
        }

    }
}
