package com.learnkafka.service;

import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.entity.LibraryEvent;
import com.learnkafka.jpa.LibraryEventsRepository;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class LibraryEventsService {

	@Autowired
	private ObjectMapper objectMapper;

	@Autowired
	private LibraryEventsRepository libraryEventsRepository;

	public void processLibraryEvent(ConsumerRecord<Integer, String> consumerRecord)
			throws JsonMappingException, JsonProcessingException {
		LibraryEvent libraryEvent = objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);
		log.info("libraryEvent: {}", libraryEvent);

		switch (libraryEvent.getLibraryEventType()) {
		case NEW:
			save(libraryEvent);
			break;
		case UPDATE:
			validate(libraryEvent);
			save(libraryEvent);
			break;
		default:
			log.error("Invalid library event type");
			break;
		}
	}

	private void validate(LibraryEvent libraryEvent) {
		if (libraryEvent.getLibraryEventId() == null) {
			throw new IllegalArgumentException("Library event id is missing!");
		}
		
		Optional<LibraryEvent> libraryEventOptional = libraryEventsRepository.findById(libraryEvent.getLibraryEventId());
		if(!libraryEventOptional.isPresent()) {
			throw new IllegalArgumentException("Not a valid library event!");
		}
		log.info("Validation is successful for the library event: {}", libraryEventOptional.get());
	}

	private void save(LibraryEvent libraryEvent) {
		libraryEvent.getBook().setLibraryEvent(libraryEvent);
		libraryEventsRepository.save(libraryEvent);
		log.info("Successfully persisted the library event: {}", libraryEvent);
	}
}
