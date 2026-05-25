package com.devalere.eventdriven.saga.payment.repository;

import com.devalere.eventdriven.saga.payment.model.Payment;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface PaymentRepository extends JpaRepository<Payment, Long> {

    Optional<Payment> findByOrderId(Long orderId);
}
