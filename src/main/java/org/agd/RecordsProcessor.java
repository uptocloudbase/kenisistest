package org.agd;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;

import java.util.List;

public class RecordsProcessor implements IRecordProcessor {
    public void initialize(String s) {
        System.out.println("== INITIALISE RECORD PROCESSOR ==");
    }

    public void processRecords(List<Record> list, IRecordProcessorCheckpointer iRecordProcessorCheckpointer) {

        System.out.println("Process Batch");
        list.forEach(r -> {
            System.out.println(r.getPartitionKey());
        });

    }

    public void shutdown(IRecordProcessorCheckpointer iRecordProcessorCheckpointer, ShutdownReason shutdownReason) {
        System.out.println("== SHUTDOWN RECORD PROCESSOR ==");
    }
}
