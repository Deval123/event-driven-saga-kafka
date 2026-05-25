package com.devalere.eventdriven.saga.saga.orchestrator;

import com.devalere.eventdriven.saga.common.SagaCommand;
import com.devalere.eventdriven.saga.common.SagaReply;
import com.devalere.eventdriven.saga.config.KafkaConfig;
import com.devalere.eventdriven.saga.saga.model.SagaState;
import com.devalere.eventdriven.saga.saga.model.SagaStateRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;

/**
 * Orchestrateur de la Saga "Commande".
 *
 * Flux Happy Path :
 *   1. CREATE_ORDER   -> OrderService
 *   2. DEBIT_PAYMENT  -> PaymentService
 *   3. RESERVE_STOCK  -> InventoryService
 *   4. CONFIRM_ORDER  -> OrderService (passe status a CONFIRMED)
 *
 * Flux Compensation (si echec) :
 *   - Si RESERVE_STOCK echoue  -> COMPENSATE DEBIT_PAYMENT, COMPENSATE CREATE_ORDER
 *   - Si DEBIT_PAYMENT echoue  -> COMPENSATE CREATE_ORDER
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class OrderSagaOrchestrator {

    private final KafkaTemplate<String, SagaCommand> kafkaTemplate;
    private final SagaStateRepository sagaStateRepository;

    // === Definition des etapes dans l'ordre ===
    private static final List<String> STEPS = List.of(
            "CREATE_ORDER",
            "DEBIT_PAYMENT",
            "RESERVE_STOCK",
            "CONFIRM_ORDER"
    );

    /**
     * Demarre une nouvelle saga pour une commande.
     */
    public SagaState startSaga(String customer, String product, int quantity, double amount) {
        String sagaId = UUID.randomUUID().toString().substring(0, 8);

        SagaState saga = SagaState.builder()
                .sagaId(sagaId)
                .customer(customer)
                .product(product)
                .quantity(quantity)
                .amount(amount)
                .currentStep("CREATE_ORDER")
                .status("STARTED")
                .build();

        sagaStateRepository.save(saga);
        log.info("[ORCHESTRATOR] Saga {} demarree pour {} - {} x{} ({}EUR)",
                sagaId, customer, product, quantity, amount);

        // Envoyer la premiere commande
        sendCommand(saga, "CREATE_ORDER", "EXECUTE");
        return saga;
    }

    /**
     * Traite la reponse d'un service.
     * Si SUCCESS -> passe a l'etape suivante.
     * Si FAILURE -> lance la compensation.
     */
    public void handleReply(SagaReply reply) {
        SagaState saga = sagaStateRepository.findById(reply.sagaId())
                .orElseThrow(() -> new RuntimeException("Saga introuvable : " + reply.sagaId()));

        if ("COMPENSATING".equals(saga.getStatus())) {
            handleCompensationReply(saga, reply);
            return;
        }

        if (reply.isSuccess()) {
            handleSuccess(saga, reply);
        } else {
            handleFailure(saga, reply);
        }
    }

    // =============================================
    //  HAPPY PATH
    // =============================================

    private void handleSuccess(SagaState saga, SagaReply reply) {
        log.info("[ORCHESTRATOR] Saga {} - Etape {} reussie (service: {})",
                saga.getSagaId(), reply.step(), reply.service());

        // Sauvegarder l'orderId si c'est le CREATE_ORDER
        if ("CREATE_ORDER".equals(reply.step()) && reply.orderId() != null) {
            saga.setOrderId(reply.orderId());
        }

        // Trouver l'etape suivante
        int currentIndex = STEPS.indexOf(reply.step());
        if (currentIndex == STEPS.size() - 1) {
            // Derniere etape = saga terminee !
            saga.setStatus("COMPLETED");
            saga.setCurrentStep("COMPLETED");
            sagaStateRepository.save(saga);
            log.info("[ORCHESTRATOR] === SAGA {} TERMINEE AVEC SUCCES === Commande #{} confirmee !",
                    saga.getSagaId(), saga.getOrderId());
        } else {
            // Passer a l'etape suivante
            String nextStep = STEPS.get(currentIndex + 1);
            saga.setCurrentStep(nextStep);
            saga.setStatus("RUNNING");
            sagaStateRepository.save(saga);
            sendCommand(saga, nextStep, "EXECUTE");
        }
    }

    // =============================================
    //  COMPENSATION
    // =============================================

    private void handleFailure(SagaState saga, SagaReply reply) {
        log.warn("[ORCHESTRATOR] Saga {} - Etape {} ECHOUEE ! Raison: {}",
                saga.getSagaId(), reply.step(), reply.reason());

        saga.setStatus("COMPENSATING");
        saga.setFailureReason(reply.reason());
        sagaStateRepository.save(saga);

        // Compenser les etapes precedentes (ordre inverse)
        int failedIndex = STEPS.indexOf(reply.step());
        if (failedIndex > 0) {
            // Compenser l'etape precedente
            String stepToCompensate = STEPS.get(failedIndex - 1);
            saga.setCurrentStep("COMPENSATE_" + stepToCompensate);
            sagaStateRepository.save(saga);
            sendCommand(saga, stepToCompensate, "COMPENSATE");
        } else {
            // Premiere etape a echoue -> rien a compenser
            saga.setStatus("FAILED");
            saga.setCurrentStep("FAILED");
            sagaStateRepository.save(saga);
            log.error("[ORCHESTRATOR] === SAGA {} ECHOUEE === Rien a compenser.", saga.getSagaId());
        }
    }

    private void handleCompensationReply(SagaState saga, SagaReply reply) {
        log.info("[ORCHESTRATOR] Saga {} - Compensation {} effectuee (service: {})",
                saga.getSagaId(), reply.step(), reply.service());

        int compensatedIndex = STEPS.indexOf(reply.step());
        if (compensatedIndex > 0) {
            // Continuer a compenser en remontant
            String nextCompensation = STEPS.get(compensatedIndex - 1);
            saga.setCurrentStep("COMPENSATE_" + nextCompensation);
            sagaStateRepository.save(saga);
            sendCommand(saga, nextCompensation, "COMPENSATE");
        } else {
            // Toutes les compensations sont faites
            saga.setStatus("FAILED");
            saga.setCurrentStep("FAILED");
            sagaStateRepository.save(saga);
            log.error("[ORCHESTRATOR] === SAGA {} ECHOUEE (compensations terminees) === Raison: {}",
                    saga.getSagaId(), saga.getFailureReason());
        }
    }

    // =============================================
    //  ENVOI DE COMMANDE
    // =============================================

    private void sendCommand(SagaState saga, String step, String action) {
        String targetService = switch (step) {
            case "CREATE_ORDER", "CONFIRM_ORDER" -> "ORDER";
            case "DEBIT_PAYMENT" -> "PAYMENT";
            case "RESERVE_STOCK" -> "INVENTORY";
            default -> throw new IllegalArgumentException("Step inconnu: " + step);
        };

        SagaCommand command = new SagaCommand(
                saga.getSagaId(),
                targetService,
                action,
                step,
                saga.getOrderId(),
                saga.getCustomer(),
                saga.getProduct(),
                saga.getQuantity(),
                saga.getAmount()
        );

        kafkaTemplate.send(KafkaConfig.SAGA_COMMANDS_TOPIC, saga.getSagaId(), command);
        log.info("[ORCHESTRATOR] Commande envoyee -> topic={}, service={}, action={}, step={}",
                KafkaConfig.SAGA_COMMANDS_TOPIC, targetService, action, step);
    }

    // =============================================
    //  CONSULTATION
    // =============================================

    public List<SagaState> getAllSagas() {
        return sagaStateRepository.findAll();
    }

    public SagaState getSaga(String sagaId) {
        return sagaStateRepository.findById(sagaId).orElse(null);
    }
}
