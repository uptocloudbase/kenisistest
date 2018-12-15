package org.agd.entity;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import org.agd.RecordsProcessor;

public class RecordsProcessorFactory implements IRecordProcessorFactory {
    @Override
    public IRecordProcessor createProcessor() {
        return new RecordsProcessor();
    }
}
