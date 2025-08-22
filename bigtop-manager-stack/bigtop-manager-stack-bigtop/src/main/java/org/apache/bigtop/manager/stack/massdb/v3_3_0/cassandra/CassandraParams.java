package org.apache.bigtop.manager.stack.massdb.v3_3_0.cassandra;

import org.apache.bigtop.manager.grpc.payload.ComponentCommandPayload;
import org.apache.bigtop.manager.stack.massdb.param.BigtopParams;
import org.apache.bigtop.manager.stack.core.annotations.GlobalParams;
import org.apache.bigtop.manager.stack.core.spi.param.Params;
import org.apache.bigtop.manager.stack.core.utils.LocalSettings;

import com.google.auto.service.AutoService;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

@Getter
@AutoService(Params.class)
@NoArgsConstructor
public class CassandraParams extends BigtopParams {

    private String cassandraDataDir = "/data/cassandra/data";
    private String cassandraCommitlogDir = "/data/cassandra/commitlog";
    private String cassandraCachesDir = "/data/cassandra/saved_caches";
    private String cassandraHintsDir = "/data/cassandra/hints";
    private String cassandraLogDir = "/var/log/cassandra";
    private String cassandraPidDir = "/var/run/cassandra";
    private String cassandraPidFile = "/var/run/cassandra/cassandra.pid";
    private String cassandraYamlContent;
    private String cassandraInContent;
    private String cassandraEnvContent;

    public CassandraParams(ComponentCommandPayload componentCommandPayload) {
        super(componentCommandPayload);
        globalParamsMap.put("java_home", javaHome());
        globalParamsMap.put("cassandra_user", user());
        globalParamsMap.put("cassandra_group", group());

    }

    @GlobalParams
    public Map<String, Object> cassandraYaml() {
        Map<String, Object> cassandraYaml = LocalSettings.configurations(getServiceName(),"cassandra.yaml");
        cassandraDataDir = (String) cassandraYaml.get("data_file_directories");
        cassandraCommitlogDir = (String) cassandraYaml.get("commitlog_directory");
        cassandraCachesDir = (String) cassandraYaml.get("saved_caches_directory");
        cassandraHintsDir = (String) cassandraYaml.get("hints_directory");
        cassandraYamlContent = (String) cassandraYaml.get("content");
        return cassandraYaml;
    }

    @GlobalParams
    public Map<String, Object> cassandraIn() {
        Map<String, Object> cassandraIn = LocalSettings.configurations(getServiceName(), "cassandra.in.sh");
        cassandraInContent = (String) cassandraIn.get("content");
        return cassandraIn;
    }

    @GlobalParams
    public Map<String, Object> cassandraEnv() {
        Map<String, Object> cassandraEnv = LocalSettings.configurations(getServiceName(), "cassandra-env.sh");
        cassandraPidDir = (String) cassandraEnv.get("cassandra_pid_dir");
        cassandraPidFile = cassandraPidDir + "/cassandra.pid";
        cassandraLogDir = (String) cassandraEnv.get("cassandra_log_dir");
        cassandraEnvContent = (String) cassandraEnv.get("content");
        return cassandraEnv;
    }

    public String cassandraBinDir() {
        return serviceHome() + "/bin";
    }

    @Override
    public String getServiceName() {
        return "cassandra";
    }
}
