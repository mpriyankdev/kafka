package com.kafka.libraryeventsconsumer.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.libraryeventsconsumer.entity.LibraryEvent;
import com.kafka.libraryeventsconsumer.jpa.LibraryEventsRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
@Slf4j
public class LibraryEventsService {

    @Autowired
    private LibraryEventsRepository libraryEventsRepository;

    @Autowired
    private ObjectMapper objectMapper;

    public void processLibraryEvent(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {

        final LibraryEvent libraryEvent = objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);
        log.info("libraryEvent : {}", libraryEvent);

        switch (libraryEvent.getLibraryEventType()) {
            case NEW:
                //save
                save(libraryEvent);
                break;
            case UPDATE:
                //update
                validate(libraryEvent);
                save(libraryEvent);
                break;
            default:
                log.info("invalid input event type");
        }

    }

    private void validate(LibraryEvent libraryEvent) {

        if (libraryEvent.getLibraryEventId() == null) {
            throw new IllegalArgumentException("Library event id is missing");
        }

        Optional<LibraryEvent> byId = libraryEventsRepository.findById(libraryEvent.getLibraryEventId());
        if (!byId.isPresent()) {
            throw new IllegalArgumentException("Not a valid library event");
        }

        log.info("validation successfull for library event : {}", libraryEvent);

    }

    private void save(LibraryEvent libraryEvent) {
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventsRepository.save(libraryEvent);
        log.info("successfully persisten the lib event : {}", libraryEvent);
    }
}
