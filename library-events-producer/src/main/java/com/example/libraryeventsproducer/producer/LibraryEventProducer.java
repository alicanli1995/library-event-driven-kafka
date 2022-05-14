package com.example.libraryeventsproducer.producer;

import com.example.libraryeventsproducer.domain.LibraryEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.List;
import java.util.concurrent.ExecutionException;

@Component
@Log4j2
@RequiredArgsConstructor
public class LibraryEventProducer {

    private final KafkaTemplate<Integer, String > kafkaTemplate;

    private static final String topicName = "library-events";
    private final ObjectMapper objectMapper;

    public void sendLibraryEventAsync(LibraryEvent libraryEvent) throws JsonProcessingException {

        Integer key = libraryEvent.getLibraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);

        var result = kafkaTemplate.sendDefault(key,value);
        result.addCallback(new ListenableFutureCallback<>() {
            @Override
            public void onFailure(Throwable ex) {
                log.error("Event send was failed. Please Try Again ! -> Message is : " + ex.getMessage());
            }

            @Override
            public void onSuccess(SendResult<Integer, String> result) {
                log.info("Event send was successfully. Event details are : Key -> {} ,  Value -> {} , Partition : {} "
                        , key, value, result.getProducerRecord().partition());
            }
        });

    }


    public void sendLibraryEventAsyncV2(LibraryEvent libraryEvent) throws JsonProcessingException {

        Integer key = libraryEvent.getLibraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);

        ProducerRecord<Integer,String> producerRecord = buildProducerRecord(key,value,topicName);
        var result = kafkaTemplate.send(producerRecord);

        result.addCallback(new ListenableFutureCallback<>() {
            @Override
            public void onFailure(Throwable ex) {
                log.error("Event send was failed. Please Try Again ! -> Message is : " + ex.getMessage());
            }

            @Override
            public void onSuccess(SendResult<Integer, String> result) {
                log.info("Event send was successfully. Event details are : Key -> {} ,  Value -> {} , Partition : {} "
                        , key, value, result.getProducerRecord().partition());
            }
        });

    }

    private ProducerRecord<Integer, String> buildProducerRecord(Integer key, String value, String topic) {


        List<Header> recordHeaders = List.of(new RecordHeader("event-source", "scanner".getBytes()));

        return new ProducerRecord<>(topic, null, key, value, recordHeaders);
    }

    public SendResult<Integer, String> sendLibraryEventSync(LibraryEvent libraryEvent) throws JsonProcessingException {

        Integer key = libraryEvent.getLibraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);
        SendResult<Integer, String> result;
        try {
            result = kafkaTemplate.sendDefault(key,value).get();

        } catch (InterruptedException | ExecutionException e) {
            log.error("InterruptedException | ExecutionException ->Event send was failed. Please Try Again ! -> Message is : " + e.getMessage());
            throw new RuntimeException(e);
        } catch (Exception e) {
            log.error("Exception ->Event send was failed. Please Try Again ! -> Message is : " + e.getMessage());
            throw new RuntimeException(e);
        }
        return result;
    }
}
