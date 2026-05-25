package com.devalere.eventdriven.saga.saga.consumer;

import com.devalere.eventdriven.saga.common.SagaReply;
import com.devalere.eventdriven.saga.config.KafkaConfig;
import com.devalere.eventdriven.saga.saga.orchestrator.OrderSagaOrchestrator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Ecoute les reponses des services sur le topic "saga-replies".
 * Transmet chaque reponse a l'orchestrateur pour qu'il decide de la suite.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class SagaReplyConsumer {

    private final OrderSagaOrchestrator orchestrator;

    @KafkaListener(topics = KafkaConfig.SAGA_REPLIES_TOPIC, groupId = "saga-orchestrator-group")
    public void onReply(SagaReply reply) {
        log.info("[REPLY] Reponse recue : saga={}, service={}, step={}, status={}",
                reply.sagaId(), reply.service(), reply.step(), reply.status());

        orchestrator.handleReply(reply);
    }
}
