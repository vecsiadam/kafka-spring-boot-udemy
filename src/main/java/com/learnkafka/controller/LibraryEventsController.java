package com.learnkafka.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.learnkafka.domain.LibraryEvent;

@RestController
public class LibraryEventsController {
	

	@PostMapping("/v1/library-event")
	public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody LibraryEvent libraryEvent){
		
		//invoke kafka producer
		return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
	}
	
}
