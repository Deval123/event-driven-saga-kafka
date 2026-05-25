package com.devalere.eventdriven.saga.order.repository;

import com.devalere.eventdriven.saga.order.model.Order;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderRepository extends JpaRepository<Order, Long> {
}
