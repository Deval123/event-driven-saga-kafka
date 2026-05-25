package com.devalere.eventdriven.saga.saga.consumer;

import com.devalere.eventdriven.saga.common.SagaCommand;
import com.devalere.eventdriven.saga.common.SagaReply;
import com.devalere.eventdriven.saga.config.KafkaConfig;
import com.devalere.eventdriven.saga.payment.model.Payment;
import com.devalere.eventdriven.saga.payment.repository.PaymentRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Simule le PaymentService qui recoit des commandes de la saga.
 * - DEBIT_PAYMENT (EXECUTE)   : debite le paiement
 * - DEBIT_PAYMENT (COMPENSATE): rembourse le paiement
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class PaymentServiceConsumer {

    private final PaymentRepository paymentRepository;
    private final KafkaTemplate<String, SagaReply> replyTemplate;

    @KafkaListener(topics = KafkaConfig.SAGA_COMMANDS_TOPIC, groupId = "payment-service-group",
            containerFactory = "commandListenerFactory")
    public void onCommand(SagaCommand command) {
        if (!"PAYMENT".equals(command.targetService())) return;

        log.info("[PAYMENT-SERVICE] Commande recue : saga={}, action={}, step={}",
                command.sagaId(), command.action(), command.step());

        try {
            if ("EXECUTE".equals(command.action())) {
                handleExecute(command);
            } else if ("COMPENSATE".equals(command.action())) {
                handleCompensate(command);
            }
        } catch (Exception e) {
            log.error("[PAYMENT-SERVICE] Erreur : {}", e.getMessage());
            sendReply(SagaReply.failure(command.sagaId(), "PAYMENT", command.step(),
                    command.orderId(), e.getMessage()));
        }
    }

    private void handleExecute(SagaCommand command) {
        // Simuler un echec si le montant depasse 10000 EUR (pour la demo)
        if (command.amount() > 10000) {
            log.warn("[PAYMENT-SERVICE] Paiement refuse ! Montant trop eleve : {}EUR", command.amount());
            sendReply(SagaReply.failure(command.sagaId(), "PAYMENT", command.step(),
                    command.orderId(), "Montant depasse la limite (10000 EUR)"));
            return;
        }

        Payment payment = Payment.builder()
                .orderId(command.orderId())
                .customer(command.customer())
                .amount(command.amount())
                .status("DEBITED")
                .build();
        paymentRepository.save(payment);

        log.info("[PAYMENT-SERVICE] Paiement debite : {}EUR pour commande #{}",
                payment.getAmount(), payment.getOrderId());

        sendReply(SagaReply.success(command.sagaId(), "PAYMENT", command.step(), command.orderId()));
    }

    private void handleCompensate(SagaCommand command) {
        paymentRepository.findByOrderId(command.orderId()).ifPresent(payment -> {
            payment.setStatus("REFUNDED");
            paymentRepository.save(payment);
            log.info("[PAYMENT-SERVICE] COMPENSATION : Paiement rembourse ({}EUR) pour commande #{}",
                    payment.getAmount(), payment.getOrderId());
        });

        sendReply(SagaReply.success(command.sagaId(), "PAYMENT", command.step(), command.orderId()));
    }

    private void sendReply(SagaReply reply) {
        replyTemplate.send(KafkaConfig.SAGA_REPLIES_TOPIC, reply.sagaId(), reply);
    }
}
