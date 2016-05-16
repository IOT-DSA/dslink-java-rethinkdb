package org.dsa.iot.rethinkdb;

import com.rethinkdb.RethinkDB;
import com.rethinkdb.gen.ast.*;
import com.rethinkdb.net.Connection;
import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.value.Value;
import org.dsa.iot.dslink.node.value.ValueType;
import org.dsa.iot.dslink.node.value.ValueUtils;
import org.dsa.iot.dslink.util.Objects;
import org.dsa.iot.dslink.util.handler.CompleteHandler;
import org.dsa.iot.historian.database.Database;
import org.dsa.iot.historian.database.DatabaseProvider;
import org.dsa.iot.historian.utils.QueryData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class HistoryDb extends Database {
    private static final Logger LOGGER = LoggerFactory.getLogger(HistoryDb.class);

    private String host;
    private int port;
    private String database;
    private String table;

    private String authKey;
    private String user;
    private String password;

    private RethinkDB r = RethinkDB.r;
    private Connection connection;

    public HistoryDb(
            String name,
            DatabaseProvider provider,
            String host,
            int port,
            String database,
            String table,
            String user,
            String password,
            String authKey
    ) {
        super(name, provider);
        this.host = host;
        this.port = port;
        this.database = database;
        this.table = table;
        this.user = user;
        this.password = password;
        this.authKey = authKey;
    }

    @Override
    public void write(String path, Value value, long ts) {
        HashMap<String, Object> obj = new HashMap<>();
        obj.put("path", path);
        obj.put("timestamp", ts);
        obj.put("value", ValueUtils.toObject(value));
        r.db(database).table(table).insert(obj).run(connection);
    }

    @Override
    public void query(String path, final long from, final long to, CompleteHandler<QueryData> handler) {
        OrderBy ob = r.db(database).table(table).filter(r.hashMap("path", path)).orderBy(r.asc("timestamp"));
        ArrayList list = ob.filter(new ReqlFunction1() {
            @Override
            public Object apply(ReqlExpr arg1) {
                return arg1.getField("timestamp").le(to).and(arg1.getField("timestamp").ge(from));
            }
        }).run(connection);

        for (Object o : list) {
            HashMap map = (HashMap) o;
            QueryData data = new QueryData();
            data.setValue(ValueUtils.toValue(map.get("value")));
            data.setTimestamp((Long) map.get("timestamp"));
            handler.handle(data);
        }
        handler.complete();
    }

    @Override
    public QueryData queryFirst(String path) {
        ArrayList<HashMap<String, Object>> list = r
                .db(database)
                .table(table)
                .filter(r.hashMap("path", path))
                .orderBy(r.asc("timestamp"))
                .limit(1)
                .run(connection);
        if (list.isEmpty()) {
            return new QueryData();
        }
        HashMap<String, Object> obj = list.get(0);
        return new QueryData(ValueUtils.toValue(obj.get("value")), (long) obj.get("timestamp"));
    }

    @Override
    public QueryData queryLast(final String path) {
        ArrayList<HashMap> list = r
                .db(database)
                .table(table)
                .filter(r.hashMap("path", path))
                .orderBy(r.desc("timestamp"))
                .limit(1)
                .run(connection);
        if (list.isEmpty()) {
            return new QueryData();
        }
        HashMap obj = list.get(0);
        return new QueryData(ValueUtils.toValue(obj.get("value")), (long) obj.get("timestamp"));
    }

    @Override
    public void close() throws Exception {
        connection.close();

        if (tickTimer != null) {
            tickTimer.cancel(true);
            tickTimer = null;
        }
    }

    @Override
    protected void performConnect() throws Exception {
        Connection.Builder cBuilder = r
                .connection()
                .hostname(host)
                .port(port)
                .db(database);

        if (user != null && password != null && !user.isEmpty()) {
            cBuilder.user(user, password);
        }

        if (authKey != null && !authKey.isEmpty()) {
            cBuilder.authKey(authKey);
        }

        connection = cBuilder.connect();

        {
            DbList list = r.dbList();
            Object result = list.contains(database).run(connection);
            if (!((Boolean) result)) {
                r.dbCreate(database).run(connection);
            }
        }

        {
            TableList list = r.db(database).tableList();
            Object result = list.contains(table).run(connection);
            if (!((Boolean) result)) {
                r.db(database).tableCreate(table).run(connection);
            }
        }
    }

    @Override
    public void initExtensions(Node node) {
        final ScheduledThreadPoolExecutor stpe = Objects.getDaemonThreadPool();

        statusNode = node
                .createChild("status")
                .setValueType(ValueType.makeBool("Connected", "Disconnected"))
                .setDisplayName("Status")
                .build();

        tickTimer = stpe.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                if (connection.isOpen()) {
                    statusNode.setValue(trueValue);
                } else {
                    statusNode.setValue(falseValue);
                }
            }
        }, 0, 1000, TimeUnit.MILLISECONDS);
    }

    private static Value falseValue = new Value(false);
    private static Value trueValue = new Value(true);
    private ScheduledFuture tickTimer;

    private Node statusNode;
}
