package com.kafka.libraryeventsproducer.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.libraryeventsproducer.domain.Book;
import com.kafka.libraryeventsproducer.domain.LibraryEvent;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.SettableListenableFuture;

import java.util.concurrent.ExecutionException;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LibraryEventProducerTest {

    @InjectMocks
    LibraryEventProducer libraryEventProducer;

    @Mock
    KafkaTemplate<Integer, String> kafkaTemplate;

    @Spy
    ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void sendLibraryEvent_failure() throws JsonProcessingException, ExecutionException, InterruptedException {

        Book book = Book.builder().bookId(123).bookAuthor("xyz").bookName("kafka using spring boot").build();
        LibraryEvent libraryEvent = LibraryEvent.builder().libraryEventId(null).book(book).build();

        SettableListenableFuture future = new SettableListenableFuture();  //for setting the listenable future
        future.setException(new RuntimeException("exception calling kafka"));

        when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(future);

        Assertions.assertThrows(Exception.class, () -> libraryEventProducer.sendLibraryEvent_producerRecord(libraryEvent).get());

    }

    @Test
    void sendLibraryEvent_success() throws JsonProcessingException, ExecutionException, InterruptedException {

        Book book = Book.builder().bookId(123).bookAuthor("xyz").bookName("kafka using spring boot").build();
        LibraryEvent libraryEvent = LibraryEvent.builder().libraryEventId(null).book(book).build();

        SettableListenableFuture future = new SettableListenableFuture();  //for setting the listenable future

        ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>("library-events", libraryEvent.getLibraryEventId(), objectMapper.writeValueAsString(libraryEvent));
        RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition("library-events", 1), 1, 1, System.currentTimeMillis(), (long) 333, 1, 2);
        SendResult<Integer, String> sendResult = new SendResult<>(producerRecord, recordMetadata);

        future.set(sendResult);

        when(kafkaTemplate.send(isA(ProducerRecord.class))).thenReturn(future);

        ListenableFuture<SendResult<Integer, String>> sendResultListenableFuture = libraryEventProducer.sendLibraryEvent_producerRecord(libraryEvent);
        SendResult<Integer, String> sendResult1 = sendResultListenableFuture.get();

        assert sendResult1.getRecordMetadata().partition() == 1;


    }

}