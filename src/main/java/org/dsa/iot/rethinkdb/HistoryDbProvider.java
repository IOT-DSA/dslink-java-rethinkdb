package org.dsa.iot.rethinkdb;

import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.NodeBuilder;
import org.dsa.iot.dslink.node.Permission;
import org.dsa.iot.dslink.node.actions.Action;
import org.dsa.iot.dslink.node.actions.ActionResult;
import org.dsa.iot.dslink.node.actions.Parameter;
import org.dsa.iot.dslink.node.value.Value;
import org.dsa.iot.dslink.node.value.ValueType;
import org.dsa.iot.dslink.util.StringUtils;
import org.dsa.iot.dslink.util.handler.Handler;
import org.dsa.iot.historian.database.Database;
import org.dsa.iot.historian.database.DatabaseProvider;

public class HistoryDbProvider extends DatabaseProvider {
    @SuppressWarnings("Duplicates")
    @Override
    public Action createDbAction(Permission perm) {
        Action act = new Action(perm, new Handler<ActionResult>() {
            @Override
            public void handle(ActionResult event) {
                Value vHost = event.getParameter("Host", ValueType.STRING);
                Value vPort = event.getParameter("Port", ValueType.NUMBER);
                Value vDatabase = event.getParameter("Database", ValueType.STRING);
                Value vTable = event.getParameter("Table", ValueType.STRING);
                Value vUser = event.getParameter("User", ValueType.STRING);
                Value vPassword = event.getParameter("Password", ValueType.STRING);
                Value vAuthKey = event.getParameter("AuthKey", ValueType.STRING);

                String name = StringUtils.encodeName(
                        vHost.getString() + ":" + vPort.getNumber().toString()
                );
                NodeBuilder builder = createDbNode(name, event);
                builder.setRoConfig("host", vHost);
                builder.setRoConfig("port", vPort);
                builder.setRoConfig("database", vDatabase);
                builder.setRoConfig("table", vTable);
                builder.setRoConfig("dbUser", vUser);
                builder.setRoConfig("dbPassword", vPassword);
                builder.setRoConfig("dbAuthKey", vAuthKey);

                Value vName = event.getParameter("Name");
                if (vName != null) {
                    builder.setDisplayName(vName.getString());
                } else {
                    builder.setDisplayName(vDatabase.getString());
                }

                createAndInitDb(builder.build());
            }
        });

        {
            Parameter p = new Parameter("Name", ValueType.STRING);
            String desc = "Name of the database\n";
            desc += "If name is not provided, the path will be used for the name";
            p.setDescription(desc);
            act.addParameter(p);

            {
                Value def = new Value("localhost");
                p = new Parameter("Host", ValueType.STRING, def);
                desc = "RethinkDB Host";
                p.setDescription(desc);
                act.addParameter(p);
            }

            {
                Value def = new Value(20815);
                p = new Parameter("Port", ValueType.NUMBER, def);
                desc = "RethinkDB Driver Port";
                p.setDescription(desc);
                act.addParameter(p);
            }

            {
                Value def = new Value("dsa");
                p = new Parameter("Database", ValueType.STRING, def);
                desc = "RethinkDB database name\n";
                desc += "If the database does not exist, it will be created";
                p.setDescription(desc);
                act.addParameter(p);
            }

            {
                Value def = new Value("history");
                p = new Parameter("Table", ValueType.STRING, def);
                desc = "RethinkDB table name\n";
                desc += "If the table does not exist, it will be created";
                p.setDescription(desc);
                act.addParameter(p);
            }
        }
        return act;
    }

    @Override
    protected Database createDb(Node node) {
        String host = node.getRoConfig("host").getString();
        Integer port = (Integer) node.getRoConfig("port").getNumber();
        String database = node.getRoConfig("database").getString();
        String table = node.getRoConfig("table").getString();
        String user = node.getRoConfig("dbUser").getString();
        String password = node.getRoConfig("dbPassword").getString();
        String authKey = node.getRoConfig("dbAuthKey").getString();

        return new HistoryDb(
                node.getDisplayName(),
                this,
                host,
                port,
                database,
                table,
                user,
                password,
                authKey
        );
    }

    @Override
    public Permission dbPermission() {
        return Permission.CONFIG;
    }
}
