package com.example.libraryeventsconsumer.service;


import com.example.libraryeventsconsumer.domain.LibraryEvent;
import com.example.libraryeventsconsumer.domain.LibraryEventType;
import com.example.libraryeventsconsumer.repository.BookRepository;
import com.example.libraryeventsconsumer.repository.LibraryEventRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;

@Service
@Log4j2
@RequiredArgsConstructor
public class LibraryEventsService {

    private final LibraryEventRepository libraryEventRepository;
    private final ObjectMapper objectMapper;

    public void processLibraryEvent(ConsumerRecord<Integer,String > records) throws JsonProcessingException {
        var event = objectMapper.readValue(records.value(), LibraryEvent.class);
        log.info("libraryEvent -> {}", event );
        if (event.getLibraryEventType().equals(LibraryEventType.NEW)) {
            save(event);
        }
        else {
            validate(event);
            save(event);
        }
    }

    private void validate(LibraryEvent event) {
        if(event.getLibraryEventId() == null ) {
            throw new IllegalArgumentException("Library Event Id is missing...");
        }
        var optional = libraryEventRepository.findById(event.getLibraryEventId());
        if(optional.isEmpty()){
            throw new IllegalArgumentException("Not a valid library event...");
        }
        log.info("Validation is successfully for the library event : {} " , event.getLibraryEventId());
    }

    private void save(LibraryEvent event) {
        event.getBook().setLibraryEvent(event);
        libraryEventRepository.save(event);
        log.info("Successfully persisted event. -> {}" ,event );
    }


}
