package sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.apache.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Map;

public class KuduSink extends RichSinkFunction<Map<String, Object>> {

    private final static Logger logger = Logger.getLogger(KuduSink.class);

    private KuduClient client;
    private KuduTable table;

    private String kuduMaster;
    private String tableName;
    private Schema schema;
    private KuduSession kuduSession;
    private ByteArrayOutputStream out;
    private ObjectOutputStream os;


    public KuduSink(String kuduMaster, String tableName) {
        this.kuduMaster = kuduMaster;
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        out = new ByteArrayOutputStream();
        os = new ObjectOutputStream(out);
        client = new KuduClient.KuduClientBuilder(kuduMaster).build();
        table = client.openTable(tableName);
        schema = table.getSchema();
        kuduSession = client.newSession();
        kuduSession.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
    }

    @Override
    public void invoke(Map<String, Object> map) {
        if (map == null) {
            return;
        }
        try {
            int columnCount = schema.getColumnCount();
            Insert insert = table.newInsert();
            PartialRow row = insert.getRow();
            for (int i = 0; i < columnCount; i++) {
                Object value = map.get(schema.getColumnByIndex(i).getName());
                insertData(row, schema.getColumnByIndex(i).getType(), schema.getColumnByIndex(i).getName(), value);
            }

            OperationResponse response = kuduSession.apply(insert);
            if (response != null) {
                logger.error(response.getRowError().toString());
            }
        } catch (Exception e) {
            logger.error(e);
        }
    }

    @Override
    public void close() throws Exception {
        try {
            kuduSession.close();
            client.close();
            os.close();
            out.close();
        } catch (Exception e) {
            logger.error(e);
        }
    }

    // 插入数据
    private void insertData(PartialRow row, Type type, String columnName, Object value) throws IOException {

        try {
            switch (type) {
                case STRING:
                    row.addString(columnName, value.toString());
                    return;
                case INT32:
                    row.addInt(columnName, Integer.valueOf(value.toString()));
                    return;
                case INT64:
                    row.addLong(columnName, Long.valueOf(value.toString()));
                    return;
                case DOUBLE:
                    row.addDouble(columnName, Double.valueOf(value.toString()));
                    return;
                case BOOL:
                    row.addBoolean(columnName, (Boolean) value);
                    return;
                case INT8:
                    row.addByte(columnName, (byte) value);
                    return;
                case INT16:
                    row.addShort(columnName, (short) value);
                    return;
                case BINARY:
                    os.writeObject(value);
                    row.addBinary(columnName, out.toByteArray());
                    return;
                case FLOAT:
                    row.addFloat(columnName, Float.valueOf(String.valueOf(value)));
                    return;
                default:
                    throw new UnsupportedOperationException("Unknown type " + type);
            }
        } catch (Exception e) {
            logger.error("数据插入异常", e);
        }
    }
}