package com.github.ralfspoeth.jdbccp;

import java.sql.*;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Pool {

    public void shutdown() {
        lock.lock();
        try {
            stopped = true;
            connectionsRequired.signal();
        }
        finally {
            lock.unlock();
        }
    }

    private class ForwardingConnection implements Connection {
        public Statement createStatement() throws SQLException {
            var stmt = conn.createStatement();
            statements.add(stmt);
            return stmt;
        }

        public PreparedStatement prepareStatement(String sql) throws SQLException {
            var ps = conn.prepareStatement(sql);
            statements.add(ps);
            return ps;
        }

        public CallableStatement prepareCall(String sql) throws SQLException {
            var cs = conn.prepareCall(sql);
            statements.add(cs);
            return cs;
        }

        public String nativeSQL(String sql) throws SQLException {
            return conn.nativeSQL(sql);
        }

        public void setAutoCommit(boolean autoCommit) throws SQLException {
            conn.setAutoCommit(autoCommit);
        }

        public boolean getAutoCommit() throws SQLException {
            return conn.getAutoCommit();
        }

        public void commit() throws SQLException {
            conn.commit();
        }

        public void rollback() throws SQLException {
            conn.rollback();
        }

        public DatabaseMetaData getMetaData() throws SQLException {
            return conn.getMetaData();
        }

        public void setReadOnly(boolean readOnly) throws SQLException {
            conn.setReadOnly(readOnly);
        }

        public boolean isReadOnly() throws SQLException {
            return conn.isReadOnly();
        }

        public void setCatalog(String catalog) throws SQLException {
            conn.setCatalog(catalog);
        }

        public String getCatalog() throws SQLException {
            return conn.getCatalog();
        }

        public void setTransactionIsolation(int level) throws SQLException {
            conn.setTransactionIsolation(level);
        }

        public int getTransactionIsolation() throws SQLException {
            return conn.getTransactionIsolation();
        }

        public SQLWarning getWarnings() throws SQLException {
            return conn.getWarnings();
        }

        public void clearWarnings() throws SQLException {
            conn.clearWarnings();
        }

        public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
            var stmt = conn.createStatement(resultSetType, resultSetConcurrency);
            statements.add(stmt);
            return stmt;
        }

        public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
            var ps = conn.prepareStatement(sql, resultSetType, resultSetConcurrency);
            statements.add(ps);
            return ps;
        }

        public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
            var cs = conn.prepareCall(sql, resultSetType, resultSetConcurrency);
            statements.add(cs);
            return cs;
        }

        public Map<String, Class<?>> getTypeMap() throws SQLException {
            return conn.getTypeMap();
        }

        public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
            conn.setTypeMap(map);
        }

        public void setHoldability(int holdability) throws SQLException {
            conn.setHoldability(holdability);
        }

        public int getHoldability() throws SQLException {
            return conn.getHoldability();
        }

        public Savepoint setSavepoint() throws SQLException {
            return conn.setSavepoint();
        }

        public Savepoint setSavepoint(String name) throws SQLException {
            return conn.setSavepoint(name);
        }

        public void rollback(Savepoint savepoint) throws SQLException {
            conn.rollback(savepoint);
        }

        public void releaseSavepoint(Savepoint savepoint) throws SQLException {
            conn.releaseSavepoint(savepoint);
        }

        public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
            var stmt = conn.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
            statements.add(stmt);
            return stmt;
        }

        public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
            var ps = conn.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
            statements.add(ps);
            return ps;
        }

        public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
            var cs = conn.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
            statements.add(cs);
            return cs;
        }

        public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
            var ps = conn.prepareStatement(sql, autoGeneratedKeys);
            statements.add(ps);
            return ps;
        }

        public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
            var ps = conn.prepareStatement(sql, columnIndexes);
            statements.add(ps);
            return ps;
        }

        public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
            var ps = conn.prepareStatement(sql, columnNames);
            statements.add(ps);
            return ps;
        }

        public Clob createClob() throws SQLException {
            return conn.createClob();
        }

        public Blob createBlob() throws SQLException {
            return conn.createBlob();
        }

        public NClob createNClob() throws SQLException {
            return conn.createNClob();
        }

        public SQLXML createSQLXML() throws SQLException {
            return conn.createSQLXML();
        }

        public boolean isValid(int timeout) throws SQLException {
            return conn.isValid(timeout);
        }

        public void setClientInfo(String name, String value) throws SQLClientInfoException {
            conn.setClientInfo(name, value);
        }

        public void setClientInfo(Properties properties) throws SQLClientInfoException {
            conn.setClientInfo(properties);
        }

        public String getClientInfo(String name) throws SQLException {
            return conn.getClientInfo(name);
        }

        public Properties getClientInfo() throws SQLException {
            return conn.getClientInfo();
        }

        public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
            return conn.createArrayOf(typeName, elements);
        }

        public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
            return conn.createStruct(typeName, attributes);
        }

        public void setSchema(String schema) throws SQLException {
            conn.setSchema(schema);
        }

        public String getSchema() throws SQLException {
            return conn.getSchema();
        }

        public void abort(Executor executor) throws SQLException {
            conn.abort(executor);
        }

        public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
            conn.setNetworkTimeout(executor, milliseconds);
        }

        public int getNetworkTimeout() throws SQLException {
            return conn.getNetworkTimeout();
        }

        public void beginRequest() throws SQLException {
            conn.beginRequest();
        }

        public void endRequest() throws SQLException {
            conn.endRequest();
        }

        public boolean setShardingKeyIfValid(ShardingKey shardingKey, ShardingKey superShardingKey, int timeout) throws SQLException {
            return conn.setShardingKeyIfValid(shardingKey, superShardingKey, timeout);
        }

        public boolean setShardingKeyIfValid(ShardingKey shardingKey, int timeout) throws SQLException {
            return conn.setShardingKeyIfValid(shardingKey, timeout);
        }

        public void setShardingKey(ShardingKey shardingKey, ShardingKey superShardingKey) throws SQLException {
            conn.setShardingKey(shardingKey, superShardingKey);
        }

        public void setShardingKey(ShardingKey shardingKey) throws SQLException {
            conn.setShardingKey(shardingKey);
        }

        public <T> T unwrap(Class<T> iface) throws SQLException {
            return isWrapperFor(iface) ? (T) conn : null;
        }

        public boolean isWrapperFor(Class<?> iface) throws SQLException {
            return Connection.class.isAssignableFrom(iface);
        }

        private final Connection conn;
        private transient boolean closed;
        private List<Statement> statements = new LinkedList<>();

        public void close() throws SQLException {
            try {
                for (var s : statements) {
                    if (!s.isClosed()) {
                        s.close();
                    }
                }
                if (conn.getAutoCommit()) {
                    conn.commit();
                } else {
                    conn.rollback();
                }
            } finally {
                statements.clear();
                closed = true;
                if (!conn.isClosed() && conn.isValid(0)) {
                    pool.add(this);
                }
            }
        }

        public boolean isClosed() {
            return closed;
        }

        private ForwardingConnection(Connection conn) {
            this.conn = conn;
        }

    }

    private final Lock lock = new ReentrantLock();
    private final Condition connectionsAvailable = lock.newCondition();
    private final Condition connectionsRequired = lock.newCondition();

    private final Deque<ForwardingConnection> pool = new LinkedList<>();

    private boolean checkConnectionsAvailable() {
        return !pool.isEmpty();
    }


    private volatile boolean stopped = false;

    public Connection get() throws InterruptedException {
        try {
            lock.lock();
            connectionsRequired.signal();
            while (pool.isEmpty()) {
                connectionsRequired.signal();
                connectionsAvailable.await();
            }
            return pool.removeFirst();
        } finally {
            connectionsRequired.signal();
            lock.unlock();
        }
    }

    private void release(ForwardingConnection conn) {
        lock.lock();
        try {
            pool.add(conn);
            connectionsAvailable.signal();
        } finally {
            lock.unlock();
        }
    }

    private final Driver jdbcDriver;
    private final String jdbcUrl;
    private final Properties info;

    private Pool(Driver drv, String jdbcUrl, Properties info) {
        this.jdbcDriver = drv;
        this.jdbcUrl = jdbcUrl;
        this.info = info;
    }

    private Connection connect() throws SQLException {
        return jdbcDriver.connect(jdbcUrl, info);
    }

    private boolean createNewPooledConnection() throws SQLException {
        return pool.add(new ForwardingConnection(connect()));
    }

    private class Producer implements Runnable {
        @Override
        public void run() {
            while(!stopped) {
                lock.lock();
                try {
                    while(!pool.isEmpty()) {
                        connectionsRequired.await();
                    }
                    try {
                        createNewPooledConnection();
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
                catch (InterruptedException iex) {
                    Thread.currentThread().interrupt();
                    stopped = true;
                }
                finally {
                    lock.unlock();
                }
            }
        }
    }

    private Thread createProducer(String jdbcUrl) {
        return Thread.ofVirtual()
                .name("connection producer for " + jdbcUrl)
                .start(new Producer());
    }

    public static Pool start(String jdbcUrl, Properties props) throws SQLException {
        // copy properties
        var info = new Properties();
        props.stringPropertyNames().forEach(e -> info.setProperty(e, props.getProperty(e)));
        // driver
        Driver jdbcDriver = DriverManager.getDriver(jdbcUrl);

        if (jdbcDriver == null) {
            throw new SQLException("No suitable driver for " + jdbcUrl);
        }

        // instantiate pool
        var p = new Pool(jdbcDriver, jdbcUrl, info);
        var thread = p.createProducer(jdbcUrl);

        // return pool
        return p;
    }
}
