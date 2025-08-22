package org.apache.bigtop.manager.stack.massdb.v3_3_0.cassandra;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CassandraParamsTest {

    private CassandraParams cassandraParams;

    @BeforeEach
    public void setUp() {
        cassandraParams = mock(CassandraParams.class);
        when(cassandraParams.stackHome()).thenReturn("/stack");
        when(cassandraParams.getServiceName()).thenCallRealMethod();
        when(cassandraParams.serviceHome()).thenCallRealMethod();
        when(cassandraParams.confDir()).thenCallRealMethod();
    }

    @Test
    public void TestServiceHome() {
        assertEquals("/stack/cassandra", cassandraParams.getServiceName());
    }

    @Test
    public void TestConfDir() {
        assertEquals("/stack/cassandra/conf", cassandraParams.confDir());
    }

    @Test
    public void TestGetServiceName() {
        assertEquals("cassandra", cassandraParams.getServiceName());
    }

}