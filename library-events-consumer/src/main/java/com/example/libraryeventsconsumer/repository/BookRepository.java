package com.example.libraryeventsconsumer.repository;

import com.example.libraryeventsconsumer.domain.Book;
import org.springframework.data.jpa.repository.JpaRepository;

public interface BookRepository extends JpaRepository<Book,Integer> {

}
