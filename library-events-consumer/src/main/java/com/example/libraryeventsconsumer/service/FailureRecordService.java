package com.example.libraryeventsconsumer.service;

import com.example.libraryeventsconsumer.domain.FailureRecord;
import com.example.libraryeventsconsumer.repository.FailureRecordRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;

@Service
@Log4j2
public class FailureRecordService {

    private FailureRecordRepository failureRecordRepository;

    public FailureRecordService(FailureRecordRepository failureRecordRepository) {
        this.failureRecordRepository = failureRecordRepository;
    }

    public void saveFailedRecord(ConsumerRecord<Integer, String> record, Exception exception, String recordStatus){
        var failureRecord = new FailureRecord(
                null,
                record.topic(),
                record.key(),
                record.value(),
                record.partition(),
                record.offset(),
                exception.getCause().getMessage(),
                recordStatus);
        failureRecordRepository.save(failureRecord);

    }
}
