package org.apache.bigtop.manager.stack.massdb.v3_3_0.seaweedfs;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.bigtop.manager.common.shell.ShellResult;
import org.apache.bigtop.manager.stack.core.exception.StackException;
import org.apache.bigtop.manager.stack.core.spi.param.Params;
import org.apache.bigtop.manager.stack.core.spi.script.AbstractServerScript;
import org.apache.bigtop.manager.stack.core.spi.script.Script;
import org.apache.bigtop.manager.stack.core.utils.linux.LinuxFileUtils;
import org.apache.bigtop.manager.stack.core.utils.linux.LinuxOSUtils;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.Properties;

@Slf4j
@AutoService(Script.class)
public class SeaweedfsFilerScript extends AbstractServerScript {

    @Override
    public ShellResult add(Params params) {
        Properties properties = new Properties();
        properties.setProperty(PROPERTY_KEY_SKIP_LEVELS, "1");
        return super.add(params, properties);
    }

    @Override
    public ShellResult start(Params params) {
        configure(params);
        SeaweedfsParams seaweedfsParams = (SeaweedfsParams) params;
        String cmd = MessageFormat.format(
                "cd {0}; nohup {0}/weed filer {1} > {2}/filer.log 2>&1 & echo -n $!>{3}",
                seaweedfsParams.serviceHome(), seaweedfsParams.getFilerStartCmd(),
                seaweedfsParams.getSeaweedfsLogDir(), seaweedfsParams.getSeaweedfsFilerPidFile());
        try {
            return LinuxOSUtils.sudoExecCmd(cmd, seaweedfsParams.user());
        } catch (IOException e) {
            throw new StackException(e);
        }
    }

    @Override
    public ShellResult stop(Params params) {
        SeaweedfsParams seaweedfsParams = (SeaweedfsParams) params;
        int pid = Integer.parseInt(LinuxFileUtils.readFile(seaweedfsParams.getSeaweedfsFilerPidFile()).replaceAll("\r|\n", ""));
        String cmd = "kill -9 " + pid;
        try {
            ShellResult result = status(seaweedfsParams);
            if (result.getExitCode() == 0) {
                return LinuxOSUtils.sudoExecCmd(cmd, seaweedfsParams.user());
            } else {
                return new ShellResult(0, "Seaweedfs Filer stopped", "");
            }
        } catch (IOException e) {
            throw new StackException(e);
        }
    }

    @Override
    public ShellResult status(Params params) {
        SeaweedfsParams seaweedfsParams = (SeaweedfsParams) params;
        return LinuxOSUtils.checkProcess(seaweedfsParams.getSeaweedfsFilerPidFile());
    }

    @Override
    public String getComponentName() {
        return "seaweedfs_filer";
    }
}
