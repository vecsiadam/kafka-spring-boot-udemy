package com.learnkafka.producer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.LibraryEvent;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class LibraryEventProducer {

	@Autowired
	private KafkaTemplate<Integer, String> kafkaTemplate;

	@Autowired
	private ObjectMapper objectMapper;

	private String topic = "library-events";

	public void sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException {
		Integer key = libraryEvent.getLibraryEventId();
		String value = objectMapper.writeValueAsString(libraryEvent.getBook());

		ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.sendDefault(key, value);
		listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {

			@Override
			public void onSuccess(SendResult<Integer, String> result) {
				handleSuccess(key, value, result);
			}

			@Override
			public void onFailure(Throwable ex) {
				handleFailure(key, value, ex);

			}

		});
	}

	public void sendLibraryEventApproach2(LibraryEvent libraryEvent) throws JsonProcessingException {
		Integer key = libraryEvent.getLibraryEventId();
		String value = objectMapper.writeValueAsString(libraryEvent);

		ProducerRecord<Integer, String> producerRecord = buildProducerRecord(key, value, topic);
		ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.send(producerRecord);
		listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {

			@Override
			public void onSuccess(SendResult<Integer, String> result) {
				handleSuccess(key, value, result);
			}

			@Override
			public void onFailure(Throwable ex) {
				handleFailure(key, value, ex);

			}

		});
	}

	private ProducerRecord<Integer, String> buildProducerRecord(Integer key, String value, String topic2) {
		List<Header> recordHeaders = List.of(new RecordHeader("event-source", "scanner".getBytes()));
		return new ProducerRecord<>(topic2, null, key, value, recordHeaders);
	}

	public SendResult<Integer, String> sendLibraryEventSynchronous(LibraryEvent libraryEvent)
			throws ExecutionException, InterruptedException, JsonProcessingException, TimeoutException {

		Integer key = libraryEvent.getLibraryEventId();
		String value = objectMapper.writeValueAsString(libraryEvent.getBook());
		SendResult<Integer, String> sendResult = null;
		try {
			sendResult = kafkaTemplate.sendDefault(key, value).get(1, TimeUnit.SECONDS);
		} catch (ExecutionException | InterruptedException e) {
			log.error("ExecutionException/InterruptedException Error sending message, the exception : {}",
					e.getMessage());
			throw e;
		} catch (Exception e) {
			log.error("Exception Error sending message, the exception : {}", e.getMessage());
			throw e;
		}

		return sendResult;
	}

	private void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {
		log.info("Message sent successfully for the key: {} and the value is {}, partitions is {}", key, value,
				result.getRecordMetadata().partition());
	}

	private void handleFailure(Integer key, String value, Throwable ex) {
		log.error("Error sending message, the exception : {}", ex.getMessage());
		try {
			throw ex;
		} catch (Throwable e) {
			log.error("Error in OnFailure: {}", e.getMessage());
		}
	}

}
