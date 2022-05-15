package com.example.libraryeventsconsumer.scheduler;

import com.example.libraryeventsconsumer.config.LibraryEventConsumerConfig;
import com.example.libraryeventsconsumer.domain.FailureRecord;
import com.example.libraryeventsconsumer.repository.FailureRecordRepository;
import com.example.libraryeventsconsumer.service.LibraryEventsService;
import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.RequiredArgsConstructor;
import lombok.extern.java.Log;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Log4j2
@Component
public class RetryScheduler {


    @Autowired
    LibraryEventsService libraryEventsService;

    @Autowired
    FailureRecordRepository failureRecordRepository;


    @Scheduled(fixedRate = 10000 )
    public void retryFailedRecords(){

        log.info("Retrying Failed Records Started!");
        var status = LibraryEventConsumerConfig.RETRY;
        failureRecordRepository.findAllByStatus(status)
                .forEach(failureRecord -> {
                    try {
                        failureRecord.setKey_record(123);
                        var consumerRecord = buildConsumerRecord(failureRecord);
                        failureRecord.setStatus(LibraryEventConsumerConfig.SUCCESS);
                        failureRecordRepository.save(failureRecord);
                        libraryEventsService.processLibraryEvent(consumerRecord);

                    } catch (Exception e){
                        log.error("Exception in retryFailedRecords : ", e);
                    }

                });

    }

    private ConsumerRecord<Integer, String> buildConsumerRecord(FailureRecord failureRecord) {

        return new ConsumerRecord<>(failureRecord.getTopic_record(),
                failureRecord.getPartition_record(), failureRecord.getOffset_value_record(), failureRecord.getKey_record(),
                failureRecord.getErrorRecord());

    }
}
