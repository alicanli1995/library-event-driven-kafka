package com.example.libraryeventsproducer.unit.producer;

import com.example.libraryeventsproducer.domain.Book;
import com.example.libraryeventsproducer.domain.LibraryEvent;
import com.example.libraryeventsproducer.producer.LibraryEventProducer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.SettableListenableFuture;

import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;


@ExtendWith(MockitoExtension.class)
class ListenerEventProducerUnitTest {

    @InjectMocks
    LibraryEventProducer libraryEventProducer;

    @Mock
    KafkaTemplate<Integer, String > kafkaTemplate;

    @Spy
    ObjectMapper objectMapper = new ObjectMapper();


    @Test
    void send_library_event_v2_fail() throws JsonProcessingException, ExecutionException, InterruptedException {

        var book = Book.builder()
                .bookAuthor("Ali")
                .bookId(123)
                .bookName("Kafka using Spring Boot")
                .build();


        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book)
                .build();

        SettableListenableFuture future = new SettableListenableFuture();

        future.setException(new RuntimeException("Exception calling Kafka"));

        when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(future);

        assertThrows(Exception.class, () -> libraryEventProducer.sendLibraryEventAsyncV2(libraryEvent).get());

    }

    @Test
    void send_library_event_v2_success() throws JsonProcessingException, ExecutionException, InterruptedException {

        var book = Book.builder()
                .bookAuthor("Ali")
                .bookId(123)
                .bookName("Kafka using Spring Boot")
                .build();


        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book)
                .build();

        var json = objectMapper.writeValueAsString(libraryEvent);

        SettableListenableFuture future = new SettableListenableFuture();

        ProducerRecord<Integer, String> producerRecord =
                new ProducerRecord("library-events", libraryEvent.getLibraryEventId(),json );


        RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition("library-events", 1),
                1,1,System.currentTimeMillis(), 1, 2);

        SendResult<Integer, String> sendResult = new SendResult<Integer, String>(producerRecord,recordMetadata);

        future.set(sendResult);

        when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(future);

        var result = libraryEventProducer.sendLibraryEventAsyncV2(libraryEvent);

        var resultGet = result.get();

        assert resultGet.getRecordMetadata().partition() == 1;

    }
}
