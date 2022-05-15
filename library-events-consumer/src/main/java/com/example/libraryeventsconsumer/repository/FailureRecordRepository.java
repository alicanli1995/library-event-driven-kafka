package com.example.libraryeventsconsumer.repository;

import com.example.libraryeventsconsumer.domain.FailureRecord;
import org.springframework.data.jpa.repository.JpaRepository;

public interface FailureRecordRepository extends JpaRepository<FailureRecord,Integer> {
}
