package org.dsa.iot.rethinkdb;

import org.dsa.iot.dslink.DSLink;
import org.dsa.iot.dslink.DSLinkFactory;
import org.dsa.iot.dslink.DSLinkHandler;
import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.NodeBuilder;
import org.dsa.iot.dslink.node.value.Value;
import org.dsa.iot.dslink.node.value.ValueType;
import org.dsa.iot.historian.Historian;
import org.dsa.iot.historian.database.DatabaseProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main extends Historian {
    private final DbProvider provider = new DbProvider();

    @Override
    public DatabaseProvider createProvider() {
        return provider;
    }

    public static void main(String[] args) {
        Main main = new Main();
        main.start(args);
    }
}
