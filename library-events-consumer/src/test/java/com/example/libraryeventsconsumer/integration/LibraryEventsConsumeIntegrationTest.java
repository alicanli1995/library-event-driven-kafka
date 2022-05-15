package com.example.libraryeventsconsumer.integration;

import com.example.libraryeventsconsumer.consumer.LibraryEventConsumer;
import com.example.libraryeventsconsumer.domain.Book;
import com.example.libraryeventsconsumer.domain.LibraryEvent;
import com.example.libraryeventsconsumer.domain.LibraryEventType;
import com.example.libraryeventsconsumer.repository.FailureRecordRepository;
import com.example.libraryeventsconsumer.repository.LibraryEventRepository;
import com.example.libraryeventsconsumer.service.LibraryEventsService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.security.oauthbearer.internals.OAuthBearerSaslServerTest;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(topics = {"library-events", "library-events-RETRY", "library-events-DLT"}, partitions = 3)
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "retryListener.startup:false"})
class LibraryEventsConsumeIntegrationTest {

    private Consumer<Integer,String > consumer;

    @Value("${topics.retry}")
    private String retryTopic;

    @Value("${topics.dlt}")
    private String deadLetterTopic;

    @SpyBean
    LibraryEventConsumer libraryEventConsumer;

    @SpyBean
    LibraryEventsService libraryEventsService;

    @Autowired
    FailureRecordRepository failureRecordRepository;

    @Autowired
    LibraryEventRepository libraryEventRepository;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    KafkaTemplate<Integer,String> kafkaTemplate;

    @Autowired
    KafkaListenerEndpointRegistry endpointRegistry;

    @BeforeEach
    void setUp(){
        var container = endpointRegistry.getListenerContainers()
                .stream()
                .filter(messageListenerContainer -> Objects.equals(messageListenerContainer.getGroupId(), "library-events-listener-group"))
                .toList()
                .get(0);
        ContainerTestUtils.waitForAssignment(container,embeddedKafkaBroker.getPartitionsPerTopic());
//        for(MessageListenerContainer messageListenerContainer : endpointRegistry.getListenerContainers()){
//            ContainerTestUtils.waitForAssignment(messageListenerContainer,embeddedKafkaBroker.getPartitionsPerTopic());
//        }
    }

    @AfterEach
    void tearDown() {
        libraryEventRepository.deleteAll();
    }

    @Test
    void publish_new_library_event_should_return_ok() throws ExecutionException, InterruptedException, JsonProcessingException {

        String json = "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":123,\"bookName\":\"Kafka using Spring Boot\",\"bookAuthor\":\"Ali\"}}";

        kafkaTemplate.sendDefault(json).get();

        CountDownLatch countDownLatch = new CountDownLatch(1);

        countDownLatch.await(3, TimeUnit.SECONDS);

        verify(libraryEventConsumer,times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsService,times(1)).processLibraryEvent(isA(ConsumerRecord.class));

        var allRecord = libraryEventRepository.findAll();

        assert allRecord.size() == 1 ;

        allRecord.forEach(libraryEvent -> {
            assert libraryEvent.getLibraryEventId() != null;
            assertEquals(123, libraryEvent.getBook().getBookId());
        });
    }

    @Test
    void publish_update_library_event_should_return_ok() throws ExecutionException, InterruptedException, JsonProcessingException {

        String json = "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
        LibraryEvent libraryEvent = objectMapper.readValue(json,LibraryEvent.class);
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventRepository.save(libraryEvent);

        var updatedBook = Book.builder()
                .bookId(456)
                .bookName("TEST - BOOK")
                .bookAuthor("Ali CANLI")
                .build();

        libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
        libraryEvent.setBook(updatedBook);

        String updatedJson = objectMapper.writeValueAsString(libraryEvent);

        kafkaTemplate.sendDefault(libraryEvent.getLibraryEventId(),updatedJson).get();

        CountDownLatch countDownLatch = new CountDownLatch(1);

        countDownLatch.await(3, TimeUnit.SECONDS);

        verify(libraryEventConsumer,times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsService,times(1)).processLibraryEvent(isA(ConsumerRecord.class));

        var response = libraryEventRepository.findById(libraryEvent.getLibraryEventId()).get();

        assertEquals("TEST - BOOK" , response.getBook().getBookName());
        assertEquals(LibraryEventType.UPDATE, response.getLibraryEventType());
    }

    @Test
    void publish_update_library_event_id_null_should_return_exception() throws ExecutionException, InterruptedException, JsonProcessingException {

        String json = "{\"libraryEventId\":null,\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";

        kafkaTemplate.sendDefault(json).get();

        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(5, TimeUnit.SECONDS);

        verify(libraryEventConsumer,times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsService,times(1)).processLibraryEvent(isA(ConsumerRecord.class));


        Map<String ,Object> configs = new HashMap<>
                (KafkaTestUtils.consumerProps("group2","true",embeddedKafkaBroker));

        consumer = new DefaultKafkaConsumerFactory<>(configs,new IntegerDeserializer(), new StringDeserializer()).createConsumer();
        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer,deadLetterTopic);


        ConsumerRecord<Integer, String> consumerRecords = KafkaTestUtils.getSingleRecord(consumer,deadLetterTopic);
        System.out.println("Consumer record ->  " + consumerRecords.value());
        assertEquals(json, consumerRecords.value());

    }

    @Test
    void publish_update_library_event_id_999_should_return_exception() throws ExecutionException, InterruptedException, JsonProcessingException {

        String json = "{\"libraryEventId\":999,\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";

        kafkaTemplate.sendDefault(json).get();

        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(5, TimeUnit.SECONDS);

        verify(libraryEventConsumer,times(3)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsService,times(3)).processLibraryEvent(isA(ConsumerRecord.class));


        Map<String ,Object> configs = new HashMap<>
                (KafkaTestUtils.consumerProps("group1","true",embeddedKafkaBroker));

        consumer = new DefaultKafkaConsumerFactory<>(configs,new IntegerDeserializer(), new StringDeserializer()).createConsumer();
        embeddedKafkaBroker.consumeFromAnEmbeddedTopic(consumer,retryTopic);


        ConsumerRecord<Integer, String> consumerRecords = KafkaTestUtils.getSingleRecord(consumer,retryTopic);
        System.out.println("Consumer record ->  " + consumerRecords.value());
        assertEquals(json, consumerRecords.value());

    }

    @Test
    void publish_update_library_event_id_null_should_return_exception_failure_record() throws ExecutionException, InterruptedException, JsonProcessingException {

        String json = "{\"libraryEventId\":null,\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";

        kafkaTemplate.sendDefault(json).get();

        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(5, TimeUnit.SECONDS);

        verify(libraryEventConsumer,times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsService,times(1)).processLibraryEvent(isA(ConsumerRecord.class));

        var count = (long) failureRecordRepository.findAll().size();
        assertEquals(1,count);

        failureRecordRepository.findAll()
                .forEach(failureRecord -> {
                    System.out.println("Failure Record ->" + failureRecord);
                });
    }

}
