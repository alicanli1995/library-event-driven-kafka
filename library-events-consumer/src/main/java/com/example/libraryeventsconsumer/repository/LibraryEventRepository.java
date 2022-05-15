package com.example.libraryeventsconsumer.repository;

import com.example.libraryeventsconsumer.domain.LibraryEvent;
import org.springframework.data.jpa.repository.JpaRepository;

public interface LibraryEventRepository extends JpaRepository<LibraryEvent,Integer> {
}
