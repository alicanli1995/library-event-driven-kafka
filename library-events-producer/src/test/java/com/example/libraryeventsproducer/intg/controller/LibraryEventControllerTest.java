package com.example.libraryeventsproducer.intg.controller;

import com.example.libraryeventsproducer.domain.Book;
import com.example.libraryeventsproducer.domain.LibraryEvent;
import com.example.libraryeventsproducer.producer.LibraryEventProducer;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
@EmbeddedKafka(topics = {"library-events"}, partitions = 3)
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.admin.properties.bootstrap.servers=${spring.embedded.kafka.brokers}"})
class LibraryEventControllerTest {

    private Consumer<Integer, String> consumer;
    @Autowired
    TestRestTemplate testRestTemplate;

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    LibraryEventProducer libraryEventProducer;

    @BeforeEach
    void setUp() {
        Map<String ,Object> configs = new HashMap<>
                (KafkaTestUtils.consumerProps("group1","true",embeddedKafkaBroker));

        consumer = new DefaultKafkaConsumerFactory<>(configs,new IntegerDeserializer(), new StringDeserializer()).createConsumer();
        embeddedKafkaBroker.consumeFromAllEmbeddedTopics(consumer);
    }

    @AfterEach
    void tearDown() {
        consumer.close();
    }

    @Test
    @Timeout(5)
    void post_library_event() {
//        GIVEN
        var book = Book.builder()
                .bookAuthor("Ali")
                .bookId(123)
                .bookName("Kafka using Spring Boot")
                .build();


        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book)
                .build();

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.set("content-type" , MediaType.APPLICATION_JSON_VALUE);
        var request = new HttpEntity<>(libraryEvent,httpHeaders);

//        WHEN
        var response = testRestTemplate.exchange
                ("/v1/libraryevent", HttpMethod.POST,request,LibraryEvent.class);

//        THEN
        assertEquals(HttpStatus.CREATED, response.getStatusCode());
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().getBook()).isEqualTo(book);

        ConsumerRecords<Integer, String> consumerRecords = KafkaTestUtils.getRecords(consumer);

        assert consumerRecords.count() == 1;
        consumerRecords.forEach(record-> {
            String expectedRecord = "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":123,\"bookName\":\"Kafka using Spring Boot\",\"bookAuthor\":\"Ali\"}}";
            String value = record.value();
            assertEquals(expectedRecord, value);
        });

    }


    @Test
    @Timeout(5)
    void put_library_event_change_information() {
//        GIVEN
        var book = Book.builder()
                .bookAuthor("Ali")
                .bookId(123)
                .bookName("Kafka using Spring Boot")
                .build();


        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(123)
                .book(book)
                .build();

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.set("content-type" , MediaType.APPLICATION_JSON_VALUE);
        var request = new HttpEntity<>(libraryEvent,httpHeaders);

//        WHEN
        var response = testRestTemplate.exchange
                ("/v1/libraryevent", HttpMethod.PUT,request,LibraryEvent.class);

//        THEN
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().getBook()).isEqualTo(book);

        ConsumerRecords<Integer, String> consumerRecords = KafkaTestUtils.getRecords(consumer);

        assert consumerRecords.count() == 1;
        consumerRecords.forEach(record-> {
            String expectedRecord = "{\"libraryEventId\":123,\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":123,\"bookName\":\"Kafka using Spring Boot\",\"bookAuthor\":\"Ali\"}}";
            String value = record.value();
            assertEquals(expectedRecord, value);
        });

    }

}
