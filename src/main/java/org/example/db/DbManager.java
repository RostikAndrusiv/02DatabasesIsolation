package org.example.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DbManager {
    private static DbManager instance = null;

    private static final String DB_URL = "jdbc:h2:mem:./test;DB_CLOSE_DELAY=-1;LOCK_MODE=1";

    private DbManager() {
    }

    public static synchronized DbManager getInstance() {
        if (instance == null) {
            instance = new DbManager();
        }
        return instance;
    }

    public Connection getConnection() throws SQLException {
        return DriverManager.getConnection(DB_URL);
    }
}
