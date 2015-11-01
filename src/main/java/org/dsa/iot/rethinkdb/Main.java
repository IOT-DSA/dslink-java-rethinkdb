package org.dsa.iot.rethinkdb;

import org.dsa.iot.historian.Historian;
import org.dsa.iot.historian.database.DatabaseProvider;

public class Main extends Historian {
    private final HistoryDbProvider provider = new HistoryDbProvider();

    @Override
    public DatabaseProvider createProvider() {
        return provider;
    }

    public static void main(String[] args) {
        Main main = new Main();
        main.start(args);
    }
}
