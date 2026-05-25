package com.devalere.eventdriven.saga.saga.consumer;

import com.devalere.eventdriven.saga.common.SagaCommand;
import com.devalere.eventdriven.saga.common.SagaReply;
import com.devalere.eventdriven.saga.config.KafkaConfig;
import com.devalere.eventdriven.saga.inventory.model.Inventory;
import com.devalere.eventdriven.saga.inventory.repository.InventoryRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Simule l'InventoryService qui recoit des commandes de la saga.
 * - RESERVE_STOCK (EXECUTE)   : reserve le stock
 * - RESERVE_STOCK (COMPENSATE): relache le stock
 *
 * Simule un echec si la quantite depasse 100 (pour la demo).
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class InventoryServiceConsumer {

    private final InventoryRepository inventoryRepository;
    private final KafkaTemplate<String, SagaReply> replyTemplate;

    @KafkaListener(topics = KafkaConfig.SAGA_COMMANDS_TOPIC, groupId = "inventory-service-group",
            containerFactory = "commandListenerFactory")
    public void onCommand(SagaCommand command) {
        if (!"INVENTORY".equals(command.targetService())) return;

        log.info("[INVENTORY-SERVICE] Commande recue : saga={}, action={}, step={}",
                command.sagaId(), command.action(), command.step());

        try {
            if ("EXECUTE".equals(command.action())) {
                handleExecute(command);
            } else if ("COMPENSATE".equals(command.action())) {
                handleCompensate(command);
            }
        } catch (Exception e) {
            log.error("[INVENTORY-SERVICE] Erreur : {}", e.getMessage());
            sendReply(SagaReply.failure(command.sagaId(), "INVENTORY", command.step(),
                    command.orderId(), e.getMessage()));
        }
    }

    private void handleExecute(SagaCommand command) {
        // Simuler un echec si la quantite depasse 100 (rupture de stock)
        if (command.quantity() > 100) {
            log.warn("[INVENTORY-SERVICE] Rupture de stock ! Quantite demandee : {}", command.quantity());
            sendReply(SagaReply.failure(command.sagaId(), "INVENTORY", command.step(),
                    command.orderId(), "Stock insuffisant (max: 100)"));
            return;
        }

        Inventory inventory = Inventory.builder()
                .orderId(command.orderId())
                .product(command.product())
                .quantity(command.quantity())
                .status("RESERVED")
                .build();
        inventoryRepository.save(inventory);

        log.info("[INVENTORY-SERVICE] Stock reserve : {} x {} pour commande #{}",
                inventory.getQuantity(), inventory.getProduct(), inventory.getOrderId());

        sendReply(SagaReply.success(command.sagaId(), "INVENTORY", command.step(), command.orderId()));
    }

    private void handleCompensate(SagaCommand command) {
        inventoryRepository.findByOrderId(command.orderId()).ifPresent(inventory -> {
            inventory.setStatus("RELEASED");
            inventoryRepository.save(inventory);
            log.info("[INVENTORY-SERVICE] COMPENSATION : Stock relache ({} x {}) pour commande #{}",
                    inventory.getQuantity(), inventory.getProduct(), inventory.getOrderId());
        });

        sendReply(SagaReply.success(command.sagaId(), "INVENTORY", command.step(), command.orderId()));
    }

    private void sendReply(SagaReply reply) {
        replyTemplate.send(KafkaConfig.SAGA_REPLIES_TOPIC, reply.sagaId(), reply);
    }
}
