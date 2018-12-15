package org.agd;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class RecordsProcessor implements IRecordProcessor {

    private List<String> processedPartitions = new ArrayList<>();
    private int recordCount = 0;

    public void initialize(String s) {
        System.out.println("== INITIALISE RECORD PROCESSOR ==");
    }

    public void processRecords(List<Record> list, IRecordProcessorCheckpointer iRecordProcessorCheckpointer) {

        System.out.println("== Process Batch ==");
        list.forEach(r -> {

            String partitionKey = r.getPartitionKey();
            String data = null;
            try {
                data = new String(r.getData().array(), "utf-8");
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
            System.out.println(partitionKey + " : " + data);

            if (!processedPartitions.contains(partitionKey)) {
                processedPartitions.add(partitionKey);
            }
            recordCount ++;
            //System.out.println("Processed " + recordCount + " records.");

        });

    }

    public void shutDownReport() {

        System.out.println("== SHUTDOWN RECORD PROCESSOR ==");
        System.out.println("Processed " + recordCount + " records.");
        System.out.println("Processed the following partition keys: ");
        processedPartitions.forEach(pk -> System.out.println(" - " + pk));

    }

    public void shutdown(IRecordProcessorCheckpointer iRecordProcessorCheckpointer, ShutdownReason shutdownReason) {

        System.out.println("Shutting down because " + shutdownReason);
        shutDownReport();
    }
}
