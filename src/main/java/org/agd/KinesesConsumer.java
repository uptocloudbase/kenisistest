package org.agd;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import org.agd.entity.RecordsProcessorFactory;

public class KinesesConsumer {

    public static void main(String[] args) {

        KinesisClientLibConfiguration config =
                new KinesisClientLibConfiguration(
                        "KinesisProducerLibSampleConsumer",
                        KinesisProducerTest.STREAM,
                        new DefaultAWSCredentialsProviderChain(),
                        "KinesisProducerLibSampleConsumer")
                        .withRegionName(KinesisProducerTest.REGION)
                        .withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON);

        new Worker.Builder()
                .recordProcessorFactory(new RecordsProcessorFactory())
                .config(config)
                .build()
                .run();

    }
}
