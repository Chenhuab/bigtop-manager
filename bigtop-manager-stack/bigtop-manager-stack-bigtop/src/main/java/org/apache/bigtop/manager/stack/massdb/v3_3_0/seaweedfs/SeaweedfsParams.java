package org.apache.bigtop.manager.stack.massdb.v3_3_0.seaweedfs;

import org.apache.bigtop.manager.grpc.payload.ComponentCommandPayload;
import org.apache.bigtop.manager.stack.core.annotations.GlobalParams;
import org.apache.bigtop.manager.stack.core.utils.LocalSettings;
import org.apache.bigtop.manager.stack.massdb.param.BigtopParams;
import org.apache.bigtop.manager.stack.core.spi.param.Params;

import com.google.auto.service.AutoService;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;


@Getter
@AutoService(Params.class)
@NoArgsConstructor
public class SeaweedfsParams extends BigtopParams {

    private String seaweedfsMasterDataDir = "/data/seaweedfs/master-data";
    private String seaweedfsVolumeDataDir = "/data/seaweedfs/volume-data";
    private String seaweedfsLogDir = "/var/log/seaweedfs";
    private String seaweedfsPidDir = "/var/run/seaweedfs";
    private String seaweedfsMasterPidFile = "/var/run/seaweedfs/master.pid";
    private String seaweedfsVolumePidFile = "/var/run/seaweedfs/volume.pid";
    private String seaweedfsFilerPidFile = "/var/run/seaweedfs/filer.pid";
    private String s3JsonDir;
    private String s3Content;
    private String filerTomlContent;

    private String masterStartCmd;
    private String volumeStartCmd;
    private String filerStartCmd;
    private String envMasterStartCmd;
    private String envVolumeStartCmd;
    private String envFilerStartCmd;

    private String seaweedfsMasterPeers;

    public SeaweedfsParams(ComponentCommandPayload componentCommandPayload) {
        super(componentCommandPayload);
        globalParamsMap.put("seaweedfs_user", user());
        globalParamsMap.put("seaweedfs_group", group());
        globalParamsMap.put("java_home", javaHome());
    }

    @GlobalParams
    public Map<String, Object> seaweedfsEnv() {
        Map<String, Object> seaweedfsEnv = LocalSettings.configurations(getServiceName(), "seaweedfs-env");
        List<String> seaweedfsMasterList = LocalSettings.componentHosts("seaweedfs_master");
        String sMasterPort = (String) LocalSettings.configurations(getServiceName(), "seaweedfs-master")
                .getOrDefault("port", "9333");
        seaweedfsLogDir = (String) seaweedfsEnv.get("seaweedfs_log_dir");
        seaweedfsPidDir = (String) seaweedfsEnv.get("seaweedfs_pid_dir");
        seaweedfsMasterPidFile = seaweedfsPidDir + "/master.pid";
        seaweedfsVolumePidFile = seaweedfsPidDir + "/volume.pid";
        seaweedfsFilerPidFile = seaweedfsPidDir + "/filer.pid";
        envMasterStartCmd = (String) seaweedfsEnv.getOrDefault("seaweedfs_master_start_command", " ");
        envVolumeStartCmd = (String) seaweedfsEnv.getOrDefault("seaweedfs_volume_start_command", " ");
        envFilerStartCmd = (String) seaweedfsEnv.getOrDefault("seaweedfs_filer_start_command", " ");
        seaweedfsMasterPeers = (String) seaweedfsEnv.get("master_peers");

        StringBuilder sMasterStr = new StringBuilder();
        if (seaweedfsMasterPeers != null && !seaweedfsMasterPeers.isEmpty() && seaweedfsMasterPeers.contains("0.0.0.0")) {
            for (int i = 0; i < seaweedfsMasterList.size(); i++) {
                if (i > 0) {
                    sMasterStr.append(",");
                }
                sMasterStr.append(seaweedfsMasterList.get(i) + ":" + sMasterPort);
            }
            seaweedfsMasterPeers = sMasterStr.toString();
        }
        return seaweedfsEnv;
    }

    @GlobalParams
    public Map<String, Object> seaweedfsMaster() {
        Map<String, Object> seaweedfsMaster = LocalSettings.configurations(getServiceName(), "seaweedfs-master");
        seaweedfsMasterDataDir = (String) seaweedfsMaster.get("mdir");

        HashMap<String, Object> map = new HashMap<>(seaweedfsMaster);
        map.put("mdir", seaweedfsMasterDataDir);
        String masterHost = hostname();
        masterStartCmd = "-ip=" + masterHost + " "
                + "-peers=" + seaweedfsMasterPeers + " "
                + toCommandLineArgs(map) + " " + envMasterStartCmd;

        return seaweedfsMaster;
    }

    @GlobalParams
    public Map<String, Object> seaweedfsVolume() {
        Map<String, Object> seaweedfsVolume = LocalSettings.configurations(getServiceName(), "seaweedfs-volume");
        seaweedfsVolumeDataDir = (String) seaweedfsVolume.get("dir");
        HashMap<String, Object> map = new HashMap<>(seaweedfsVolume);
        map.put("dir", seaweedfsVolumeDataDir);
        volumeStartCmd = "-ip=" + hostname() + " "
                + "-mserver=" + seaweedfsMasterPeers + " "
                + toCommandLineArgs(map) + " " + envVolumeStartCmd;
        return seaweedfsVolume;
    }

    @GlobalParams
    public Map<String, Object> seaweedfsFiler() {
        Map<String, Object> seaweedfsFiler = LocalSettings.configurations(getServiceName(), "seaweedfs-filer");
        s3Content = (String) seaweedfsFiler.get("content");
        s3JsonDir = (String) seaweedfsFiler.getOrDefault("s3.config", "/opt/services/seaweedfs");

        if (s3JsonDir.contains("install_dir")) {
            s3JsonDir = serviceHome();
        }

        HashMap<String, Object> map = new HashMap<>(seaweedfsFiler);
        filerStartCmd = "-ip=" + hostname() + " " + "-master=" + seaweedfsMasterPeers + " ";
        String selectGatewayType = (String) map.remove("select_gateway_type");
        map.remove("s3.config");
        map.remove("content");
        if (selectGatewayType != null && selectGatewayType.equals("s3")) {
            filerStartCmd = filerStartCmd + "-s3" + " -s3.config=" + s3JsonDir + "/s3.json" + " ";
        }
        filerStartCmd = filerStartCmd + toCommandLineArgs(map) + " " + envFilerStartCmd;


        return seaweedfsFiler;
    }

    @GlobalParams
    public Map<String, Object> seaweedfsFilerToml() {
        Map<String, Object> seaweedfsFilerToml = LocalSettings.configurations(getServiceName(), "filer.toml");
        filerTomlContent = (String) seaweedfsFilerToml.get("content");
        return seaweedfsFilerToml;
    }

    public static String toCommandLineArgs(Map<String, Object> map) {
        StringBuilder result = new StringBuilder();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            if (key == null || value == null) {
                continue;
            }
            if (result.length() > 0) {
                result.append(" ");
            }
            result.append("-")
                    .append(key)
                    .append("=")
                    .append(value.toString());
        }
        return result.toString();
    }

    @Override
    public String getServiceName() {
        return "seaweedfs";
    }
}
