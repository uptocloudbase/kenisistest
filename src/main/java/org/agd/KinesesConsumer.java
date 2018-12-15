package org.agd;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import org.agd.entity.RecordsProcessorFactory;

public class KinesesConsumer {

    private static final long TIMEOUT = 180000; //3 mins

    public static void main(String[] args) throws InterruptedException {

        if (args.length != 1) {
            System.out.println("Gis us a app name buddy");
            System.exit(0);
        }

        String appName = args[0];

        KinesisClientLibConfiguration config =
                new KinesisClientLibConfiguration(
                        appName,
                        KinesisProducerTest.STREAM,
                        new DefaultAWSCredentialsProviderChain(),
                        appName)
                        .withRegionName(KinesisProducerTest.REGION)
                        .withInitialPositionInStream(InitialPositionInStream.LATEST);

        Worker worker = new Worker.Builder()
                .recordProcessorFactory(new RecordsProcessorFactory())
                .config(config)
                .build();

                new Thread(worker).start();

                Thread.sleep(TIMEOUT);

                System.out.println("Worker shutdown.");
                worker.startGracefulShutdown();
    }
}
