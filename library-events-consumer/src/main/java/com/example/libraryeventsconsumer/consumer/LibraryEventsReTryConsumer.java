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
public class LibraryEventsReTryConsumer {
    private final LibraryEventsService libraryEventsService;

    @KafkaListener(topics = {"${topics.retry}"},
            autoStartup = "${retryListener.startup:false}"
            , groupId = "retry-listener-group")
    public void onMessage(ConsumerRecord<Integer,String> consumerRecord) throws JsonProcessingException {

        log.info("ConsumerRecord in Retry Consumer: {} ", consumerRecord );
        consumerRecord.headers()
                        .forEach(header -> {
                            log.info("Key :  {} -> Value : {} " , header.key(), new String(header.value()));
                        });
        libraryEventsService.processLibraryEvent(consumerRecord);

    }

}
