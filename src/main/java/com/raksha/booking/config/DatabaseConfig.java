package com.raksha.booking.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Singleton HikariCP connection pool.
 *
 * <p>Configuration is read from {@code application.properties} on the classpath.
 * The DataSource is initialised once on class-load; all callers share the pool.
 *
 * <p>Usage:
 * <pre>
 *   try (Connection conn = DatabaseConfig.getConnection()) {
 *       // JDBC work
 *   }
 * </pre>
 */
public final class DatabaseConfig {

    private static final Logger log = LoggerFactory.getLogger(DatabaseConfig.class);
    private static final HikariDataSource DATA_SOURCE;

    static {
        Properties props = loadProperties();

        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(props.getProperty("db.url",
                "jdbc:postgresql://localhost:5432/ticketdb"));
        cfg.setUsername(props.getProperty("db.username", "raksha"));
        cfg.setPassword(props.getProperty("db.password", "ticket123"));
        cfg.setMaximumPoolSize(
                Integer.parseInt(props.getProperty("db.pool.maxSize", "20")));
        cfg.setMinimumIdle(
                Integer.parseInt(props.getProperty("db.pool.minIdle", "5")));
        cfg.setConnectionTimeout(
                Long.parseLong(props.getProperty("db.pool.connectionTimeout", "3000")));
        cfg.setPoolName("TicketPool");
        cfg.addDataSourceProperty("cachePrepStmts", "true");
        cfg.addDataSourceProperty("prepStmtCacheSize", "250");
        cfg.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");

        DATA_SOURCE = new HikariDataSource(cfg);
        log.info("HikariCP pool initialised — maxSize={}", cfg.getMaximumPoolSize());
    }

    private DatabaseConfig() {}

    /** Returns a pooled {@link Connection}. Caller must close it (try-with-resources). */
    public static Connection getConnection() throws SQLException {
        return DATA_SOURCE.getConnection();
    }

    /** Gracefully shuts down the pool. Call once on JVM exit. */
    public static void shutdown() {
        if (DATA_SOURCE != null && !DATA_SOURCE.isClosed()) {
            DATA_SOURCE.close();
            log.info("HikariCP pool shut down.");
        }
    }

    private static Properties loadProperties() {
        Properties props = new Properties();
        try (InputStream is = DatabaseConfig.class
                .getClassLoader()
                .getResourceAsStream("application.properties")) {
            if (is != null) {
                props.load(is);
            }
        } catch (IOException e) {
            log.warn("Could not load application.properties — using defaults", e);
        }
        return props;
    }
}
