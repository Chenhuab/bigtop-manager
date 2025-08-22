package org.apache.bigtop.manager.stack.massdb.v3_3_0.cassandra;

import org.apache.bigtop.manager.common.constants.Constants;
import org.apache.bigtop.manager.common.shell.ShellResult;
import org.apache.bigtop.manager.stack.core.spi.param.Params;
import org.apache.bigtop.manager.stack.core.utils.LocalSettings;
import org.apache.bigtop.manager.stack.core.utils.linux.LinuxFileUtils;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;


import java.text.MessageFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class CassandraSetup {
    public static ShellResult configure(Params params) {
        log.info("Configuring Cassandra");
        CassandraParams cassandraParams = (CassandraParams) params;

        String confDir = cassandraParams.confDir();
        String binDir = cassandraParams.cassandraBinDir();
        String cassandraUser = cassandraParams.user();
        String cassandraGroup = cassandraParams.group();
        Map<String, Object> cassandraYaml = cassandraParams.cassandraYaml();
        Map<String, Object> cassandraIn = cassandraParams.cassandraIn();
        Map<String, Object> cassandraEnv = cassandraParams.cassandraEnv();
        List<String> cassandraHostList = LocalSettings.hosts("cassandra_server");

        LinuxFileUtils.createDirectories(
                cassandraParams.getCassandraDataDir(), cassandraUser, cassandraGroup, Constants.PERMISSION_755, true);
        LinuxFileUtils.createDirectories(
                cassandraParams.getCassandraCommitlogDir(), cassandraUser, cassandraGroup, Constants.PERMISSION_755, true);
        LinuxFileUtils.createDirectories(
                cassandraParams.getCassandraCachesDir(), cassandraUser, cassandraGroup, Constants.PERMISSION_755, true);
        LinuxFileUtils.createDirectories(
                cassandraParams.getCassandraHintsDir(), cassandraUser, cassandraGroup, Constants.PERMISSION_755, true);

        HashMap<String, Object> map = new HashMap<>(cassandraYaml);
        map.remove("content");
        HashMap<String, Object> paramMap = new HashMap<>();
        paramMap.put("ca_server_list", cassandraHostList);
        paramMap.put("host", cassandraParams.hostname());
        LinuxFileUtils.toFileByTemplate(
                cassandraParams.getCassandraYamlContent(),
                MessageFormat.format("{0}/cassandra.yaml", confDir),
                cassandraUser,
                cassandraGroup,
                Constants.PERMISSION_644,
                map,
                paramMap);

        LinuxFileUtils.toFileByTemplate(
                cassandraParams.getCassandraEnvContent(),
                MessageFormat.format("{0}/cassandra-env.sh", confDir),
                cassandraUser,
                cassandraGroup,
                Constants.PERMISSION_644,
                cassandraEnv);

        LinuxFileUtils.toFileByTemplate(
                cassandraParams.getCassandraInContent(),
                MessageFormat.format("{0}/cassandra.in.sh", binDir),
                cassandraUser,
                cassandraGroup,
                Constants.PERMISSION_644,
                cassandraIn
        );
        log.info("Successfully configured Cassandra");
        return ShellResult.success();
    }
}
