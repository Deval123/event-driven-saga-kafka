package com.devalere.eventdriven.saga.saga.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * Etat persiste de la Saga.
 * L'orchestrateur sauvegarde chaque etape pour pouvoir reprendre ou compenser.
 */
@Entity
@Table(name = "saga_states")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SagaState {

    @Id
    private String sagaId;

    private Long orderId;
    private String customer;
    private String product;
    private int quantity;
    private double amount;

    /**
     * Etape courante de la saga :
     * CREATE_ORDER -> DEBIT_PAYMENT -> RESERVE_STOCK -> CONFIRM_ORDER -> COMPLETED
     */
    private String currentStep;

    /**
     * Statut global : STARTED, RUNNING, COMPLETED, COMPENSATING, FAILED
     */
    private String status;

    private String failureReason;

    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;

    @PrePersist
    void prePersist() {
        createdAt = LocalDateTime.now();
        updatedAt = LocalDateTime.now();
    }

    @PreUpdate
    void preUpdate() {
        updatedAt = LocalDateTime.now();
    }
}
