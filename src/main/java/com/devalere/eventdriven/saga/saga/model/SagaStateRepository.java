package com.devalere.eventdriven.saga.saga.model;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface SagaStateRepository extends JpaRepository<SagaState, String> {

    List<SagaState> findByStatusOrderByCreatedAtDesc(String status);
}
