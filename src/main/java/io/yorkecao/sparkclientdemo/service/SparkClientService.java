package io.yorkecao.sparkclientdemo.service;

import io.yorkecao.sparkclientdemo.config.SparkClientConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import org.springframework.stereotype.Service;

import java.io.IOException;

/**
 * @author Yorke
 */
@Slf4j
@Service
public class SparkClientService {

    private SparkClientConfiguration config;

    public SparkClientService(SparkClientConfiguration config) {
        this.config = config;
    }

    public void submit(String[] params) throws IOException {
        SparkAppHandle handle = new SparkLauncher()
                .setAppName(config.getAppName())
                .setSparkHome(config.getSparkHome())
                .setMaster(config.getMaster())
                .setConf(SparkLauncher.DRIVER_MEMORY, config.getSparkDriverMemory())
                .setConf(SparkLauncher.EXECUTOR_MEMORY, config.getSparkExecutorMemory())
                .setConf(SparkLauncher.EXECUTOR_CORES, config.getSparkExecutorCores())
                .setConf("spark.cores.max", "7")
                .setConf("spark.rpc.netty.dispatcher.numThreads", "2")
                .setAppResource(config.getAppResource())
                .setMainClass(config.getMainClass())
                .addAppArgs(params)
                .setDeployMode(config.getDeployMode())
                .startApplication(new SparkAppHandle.Listener() {

                    @Override
                    public void stateChanged(SparkAppHandle handle) {
                        log.info("State changed to {}.", handle.getState().toString());
                    }

                    @Override
                    public void infoChanged(SparkAppHandle sparkAppHandle) {
                        log.info("Info changed.");
                    }
                });

        boolean isListening = true;
        while (isListening) {
            switch (handle.getState()) {
                case FAILED:
                    throw new RuntimeException("执行失败！");
                case LOST:
                    throw new RuntimeException("任务丢失！");
                case FINISHED:
                    isListening = false;
                    break;
                default:
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        log.error(e.getMessage(), e);
                    }
            }
        }
    }
}
