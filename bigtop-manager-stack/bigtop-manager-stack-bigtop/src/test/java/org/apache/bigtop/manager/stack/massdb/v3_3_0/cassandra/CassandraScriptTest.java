package org.apache.bigtop.manager.stack.massdb.v3_3_0.cassandra;

import org.apache.bigtop.manager.stack.core.spi.param.Params;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CassandraScriptTest {

    private final CassandraScript cassandraScript = new CassandraScript();

    @Test
    public void testGetComponentName() {
        assertEquals("cassandra_server", cassandraScript.getComponentName());
    }

    @Test
    public void testAddParamsNull() {
        Params params = null;
        assertThrows(NullPointerException.class, () -> cassandraScript.add(params));
    }

    @Test
    public void testConfigureParamsNull() {
        Params params = null;
        assertThrows(NullPointerException.class, () -> cassandraScript.configure(params));
    }

    @Test
    public void testStartParamsNull() {
        Params params = null;
        assertThrows(NullPointerException.class, () -> cassandraScript.start(params));
    }

    @Test
    public void testStopParamsNull() {
        Params params = null;
        assertThrows(NullPointerException.class, () -> cassandraScript.stop(params));
    }

    @Test
    public void testStatusParamsNull() {
        Params params = null;
        assertThrows(NullPointerException.class, () -> cassandraScript.status(params));
    }
}