package org.apache.bigtop.manager.stack.massdb.v3_3_0.seaweedfs;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.bigtop.manager.common.constants.Constants;
import org.apache.bigtop.manager.common.shell.ShellResult;
import org.apache.bigtop.manager.stack.core.spi.param.Params;
import org.apache.bigtop.manager.stack.core.utils.LocalSettings;
import org.apache.bigtop.manager.stack.core.utils.linux.LinuxFileUtils;

import java.text.MessageFormat;
import java.util.List;
import java.util.Map;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class SeaweedfsSetup {

    public static ShellResult configure(Params params) {
        log.info("configuring seaweedfs");
        SeaweedfsParams seaweedfsParams = (SeaweedfsParams) params;

        String seaweedfsUser = seaweedfsParams.user();
        String seaweedfsGroup = seaweedfsParams.group();
        List<String> cassandraServer = LocalSettings.componentHosts("cassandra_server");
        String native_transport_port = (String) LocalSettings.configurations("cassandra", "cassandra.yaml").get("native_transport_port");

        LinuxFileUtils.createDirectories(
                seaweedfsParams.getSeaweedfsMasterDataDir(), seaweedfsUser, seaweedfsGroup, Constants.PERMISSION_755, true);
        LinuxFileUtils.createDirectories(
                seaweedfsParams.getSeaweedfsVolumeDataDir(), seaweedfsUser, seaweedfsGroup, Constants.PERMISSION_755, true);
        LinuxFileUtils.createDirectories(
                seaweedfsParams.getSeaweedfsPidDir(), seaweedfsUser, seaweedfsGroup, Constants.PERMISSION_755, true);
        LinuxFileUtils.createDirectories(
                seaweedfsParams.getSeaweedfsLogDir(), seaweedfsUser, seaweedfsGroup, Constants.PERMISSION_755, true);
        LinuxFileUtils.createDirectories(
                seaweedfsParams.getS3JsonDir(), seaweedfsUser, seaweedfsGroup, Constants.PERMISSION_755, true);


        Map<String, Object> paramMap = Map.of("cassandra_servers", cassandraServer, "native_transport_port", native_transport_port);
        LinuxFileUtils.toFileByTemplate(
                seaweedfsParams.getFilerTomlContent(),
                MessageFormat.format("{0}/filer.toml", seaweedfsParams.serviceHome()),
                seaweedfsUser,
                seaweedfsGroup,
                Constants.PERMISSION_644,
                paramMap);

        LinuxFileUtils.toFileByTemplate(
                seaweedfsParams.getS3Content(),
                MessageFormat.format("{0}/s3.json", seaweedfsParams.getS3JsonDir()),
                seaweedfsUser,
                seaweedfsGroup,
                Constants.PERMISSION_644,
                seaweedfsParams.getGlobalParamsMap());

        log.info("Successfully configured seaweedfs");
        return  ShellResult.success();
    }
}
