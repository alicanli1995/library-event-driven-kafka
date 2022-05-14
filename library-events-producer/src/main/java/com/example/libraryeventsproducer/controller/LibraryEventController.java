package com.example.libraryeventsproducer.controller;

import com.example.libraryeventsproducer.domain.LibraryEvent;
import com.example.libraryeventsproducer.domain.LibraryEventType;
import com.example.libraryeventsproducer.producer.LibraryEventProducer;
import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.context.annotation.RequestScope;

@RestController
@RequestScope
@Validated
@CrossOrigin
@RequiredArgsConstructor
@Log4j2
public class LibraryEventController {

    private final LibraryEventProducer libraryEventProducer;

    @PostMapping("/v1/libraryevent")
    public ResponseEntity<LibraryEvent> addLibraryEvent(@RequestBody @Validated LibraryEvent libraryEvent) throws JsonProcessingException {
        libraryEvent.setLibraryEventType(LibraryEventType.NEW);
        libraryEventProducer.sendLibraryEventAsyncV2(libraryEvent);
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }

    @PutMapping("/v1/libraryevent")
    public ResponseEntity<?> changeBookInformation(@RequestBody @Validated LibraryEvent libraryEvent) throws JsonProcessingException {
        if (libraryEvent.getLibraryEventId() == null)
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Please enter the library event id !");

        libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
        libraryEventProducer.sendLibraryEventAsyncV2(libraryEvent);
        return ResponseEntity.status(HttpStatus.OK).body(libraryEvent);
    }


}
