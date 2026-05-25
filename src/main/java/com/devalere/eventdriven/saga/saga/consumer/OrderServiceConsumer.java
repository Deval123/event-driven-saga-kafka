package com.devalere.eventdriven.saga.saga.consumer;

import com.devalere.eventdriven.saga.common.SagaCommand;
import com.devalere.eventdriven.saga.common.SagaReply;
import com.devalere.eventdriven.saga.config.KafkaConfig;
import com.devalere.eventdriven.saga.order.model.Order;
import com.devalere.eventdriven.saga.order.repository.OrderRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Simule le OrderService qui recoit des commandes de la saga.
 * - CREATE_ORDER  (EXECUTE)   : cree la commande en base
 * - CREATE_ORDER  (COMPENSATE): annule la commande
 * - CONFIRM_ORDER (EXECUTE)   : confirme la commande
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class OrderServiceConsumer {

    private final OrderRepository orderRepository;
    private final KafkaTemplate<String, SagaReply> replyTemplate;

    @KafkaListener(topics = KafkaConfig.SAGA_COMMANDS_TOPIC, groupId = "order-service-group",
            containerFactory = "commandListenerFactory")
    public void onCommand(SagaCommand command) {
        if (!"ORDER".equals(command.targetService())) return;

        log.info("[ORDER-SERVICE] Commande recue : saga={}, action={}, step={}",
                command.sagaId(), command.action(), command.step());

        try {
            if ("EXECUTE".equals(command.action())) {
                handleExecute(command);
            } else if ("COMPENSATE".equals(command.action())) {
                handleCompensate(command);
            }
        } catch (Exception e) {
            log.error("[ORDER-SERVICE] Erreur : {}", e.getMessage());
            sendReply(SagaReply.failure(command.sagaId(), "ORDER", command.step(),
                    command.orderId(), e.getMessage()));
        }
    }

    private void handleExecute(SagaCommand command) {
        if ("CREATE_ORDER".equals(command.step())) {
            Order order = Order.builder()
                    .customer(command.customer())
                    .product(command.product())
                    .quantity(command.quantity())
                    .totalAmount(command.amount())
                    .status("CREATED")
                    .build();
            order = orderRepository.save(order);

            log.info("[ORDER-SERVICE] Commande #{} creee pour {} - {} ({}EUR)",
                    order.getId(), order.getCustomer(), order.getProduct(), order.getTotalAmount());

            sendReply(SagaReply.success(command.sagaId(), "ORDER", command.step(), order.getId()));

        } else if ("CONFIRM_ORDER".equals(command.step())) {
            Order order = orderRepository.findById(command.orderId())
                    .orElseThrow(() -> new RuntimeException("Commande introuvable: " + command.orderId()));
            order.setStatus("CONFIRMED");
            orderRepository.save(order);

            log.info("[ORDER-SERVICE] Commande #{} CONFIRMEE !", order.getId());

            sendReply(SagaReply.success(command.sagaId(), "ORDER", command.step(), order.getId()));
        }
    }

    private void handleCompensate(SagaCommand command) {
        if ("CREATE_ORDER".equals(command.step())) {
            orderRepository.findById(command.orderId()).ifPresent(order -> {
                order.setStatus("CANCELLED");
                orderRepository.save(order);
                log.info("[ORDER-SERVICE] COMPENSATION : Commande #{} annulee", order.getId());
            });

            sendReply(SagaReply.success(command.sagaId(), "ORDER", command.step(), command.orderId()));
        }
    }

    private void sendReply(SagaReply reply) {
        replyTemplate.send(KafkaConfig.SAGA_REPLIES_TOPIC, reply.sagaId(), reply);
    }
}
