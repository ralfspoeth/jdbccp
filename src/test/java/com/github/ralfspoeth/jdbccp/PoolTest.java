package com.github.ralfspoeth.jdbccp;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PoolTest {

    @Test
    void testPool() throws SQLException, InterruptedException {
        var pool = Pool.start("jdbc:h2:mem:test", System.getProperties());
        try(var conn = pool.get()) {
            System.out.println(conn);
        }
        pool.shutdown();
    }

    @Test
    void testH2Driver() throws SQLException {
        assertAll(
                () -> assertNotNull(DriverManager.getConnection("jdbc:h2:mem:test"))
        );
    }
}
