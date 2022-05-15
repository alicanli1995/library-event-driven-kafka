package com.example.libraryeventsconsumer.consumer;

import com.example.libraryeventsconsumer.service.LibraryEventsService;
import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class LibraryEventConsumer {

    private final LibraryEventsService libraryEventsService;


    @KafkaListener(topics = {"library-events"} ,
            groupId = "library-events-listener-group"
    )
    public void onMessage(ConsumerRecord<Integer,String > records) throws JsonProcessingException {
        log.info("Consumer Record ReTry Topic -> " + records);
        libraryEventsService.processLibraryEvent(records);
    }

}
