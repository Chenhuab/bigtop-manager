package org.apache.bigtop.manager.stack.massdb.v3_3_0.cassandra;


import org.apache.bigtop.manager.common.shell.ShellResult;
import org.apache.bigtop.manager.stack.core.exception.StackException;
import org.apache.bigtop.manager.stack.core.spi.param.Params;
import org.apache.bigtop.manager.stack.core.spi.script.AbstractServerScript;
import org.apache.bigtop.manager.stack.core.spi.script.Script;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.bigtop.manager.stack.core.utils.linux.LinuxOSUtils;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.Properties;

@Slf4j
@AutoService(Script.class)
public class CassandraScript extends AbstractServerScript {

    @Override
    public ShellResult add(Params params) {
        Properties properties = new Properties();
        properties.setProperty(PROPERTY_KEY_SKIP_LEVELS, "1");

        return super.add(params, properties);
    }

    @Override
    public ShellResult configure(Params params) {
        super.configure(params);

        return CassandraSetup.configure(params);
    }

    @Override
    public ShellResult start(Params params) {
        configure(params);
        CassandraParams cassandraParams = (CassandraParams) params;

        String cmd = MessageFormat.format(
                "nohup {0}/bin/cassandra -R -f > {1}/cassandra.log 2>&1 & echo -n $!>{2}",
                cassandraParams.serviceHome(), cassandraParams.getCassandraLogDir(), cassandraParams.getCassandraPidFile());
        try {
            return LinuxOSUtils.sudoExecCmd(cmd, cassandraParams.user());
        } catch (IOException e) {
            throw new StackException(e);
        }
    }

    @Override
    public ShellResult stop(Params params) {
        CassandraParams cassandraParams = (CassandraParams) params;
        String cmd = "ps -ef |grep -v grep |grep CassandraDaemon | awk '{print $2}'| xargs kill -9";
        try {
            ShellResult result = status(cassandraParams);
            if (result.getExitCode() == 0) {
                return LinuxOSUtils.sudoExecCmd(cmd, cassandraParams.user());
            } else {
                return new ShellResult(0, "Cassandra Server stopped", "");
            }
        } catch (IOException e) {
            throw new StackException(e);
        }
    }

    @Override
    public ShellResult status(Params params) {
        CassandraParams cassandraParams = (CassandraParams) params;
        return LinuxOSUtils.checkProcess(cassandraParams.getCassandraPidFile());
    }

    @Override
    public String getComponentName() {
        return "cassandra_server";
    }
}
