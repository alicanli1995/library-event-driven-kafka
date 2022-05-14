package com.example.libraryeventsproducer.unit.controller;

import com.example.libraryeventsproducer.controller.LibraryEventController;
import com.example.libraryeventsproducer.domain.Book;
import com.example.libraryeventsproducer.domain.LibraryEvent;
import com.example.libraryeventsproducer.producer.LibraryEventProducer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(LibraryEventController.class)
@AutoConfigureMockMvc
class LibraryEventControllerUnitTest {

    @Autowired
    MockMvc mockMvc;

    @MockBean
    LibraryEventProducer libraryEventProducer;

    ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void post_library_event_should_return_created() throws Exception {

        var book = Book.builder()
                .bookAuthor("Ali")
                .bookId(123)
                .bookName("Kafka using Spring Boot")
                .build();


        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book)
                .build();

        var body = objectMapper.writeValueAsString(libraryEvent);

        when(libraryEventProducer.sendLibraryEventAsyncV2(isA(LibraryEvent.class))).thenReturn(null);

        mockMvc.perform(post("/v1/libraryevent")
                .content(body)
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isCreated());

    }

    @Test
    void post_library_event_should_return_error() throws Exception {

        var book = Book.builder()
                .bookAuthor(null)
                .bookId(null)
                .bookName("Kafka using Spring Boot")
                .build();


        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book)
                .build();

        var body = objectMapper.writeValueAsString(libraryEvent);
        when(libraryEventProducer.sendLibraryEventAsyncV2(isA(LibraryEvent.class))).thenReturn(null);

        String expectedErrorMessage = "book.bookAuthor - must not be blank, book.bookId - must not be null";

        mockMvc.perform(post("/v1/libraryevent")
                        .content(body)
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().is4xxClientError())
                .andExpect(content().string(expectedErrorMessage));

    }

    @Test
    void put_library_event_should_return_ok() throws Exception {

        var book = Book.builder()
                .bookAuthor("Ali")
                .bookId(123)
                .bookName("Kafka using Spring Boot")
                .build();


        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(123)
                .book(book)
                .build();

        var body = objectMapper.writeValueAsString(libraryEvent);

        when(libraryEventProducer.sendLibraryEventAsyncV2(isA(LibraryEvent.class))).thenReturn(null);

        mockMvc.perform(put("/v1/libraryevent")
                        .content(body)
                        .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk());

    }

    @Test
    void updateLibraryEvent_withNullLibraryEventId() throws Exception {

        //given
        Book book = new Book().builder()
                .bookId(123)
                .bookAuthor("Dilip")
                .bookName("Kafka Using Spring Boot")
                .build();

        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book)
                .build();
        String json = objectMapper.writeValueAsString(libraryEvent);
        when(libraryEventProducer.sendLibraryEventAsyncV2(isA(LibraryEvent.class))).thenReturn(null);

        //expect
        mockMvc.perform(
                        put("/v1/libraryevent")
                                .content(json)
                                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().is4xxClientError())
                .andExpect(content().string("Please enter the library event id !"));

    }
}
