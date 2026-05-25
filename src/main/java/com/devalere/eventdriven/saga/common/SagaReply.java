package com.devalere.eventdriven.saga.common;

/**
 * Reponse envoyee par un service vers l'orchestrateur.
 * Indique si l'etape a reussi ou echoue.
 */
public record SagaReply(
        String sagaId,
        String service,    // "ORDER", "PAYMENT", "INVENTORY"
        String step,       // "CREATE_ORDER", "DEBIT_PAYMENT", "RESERVE_STOCK", "CONFIRM_ORDER"
        String status,     // "SUCCESS" ou "FAILURE"
        String reason,     // raison de l'echec (si FAILURE)
        Long orderId
) {

    public static SagaReply success(String sagaId, String service, String step, Long orderId) {
        return new SagaReply(sagaId, service, step, "SUCCESS", null, orderId);
    }

    public static SagaReply failure(String sagaId, String service, String step, Long orderId, String reason) {
        return new SagaReply(sagaId, service, step, "FAILURE", reason, orderId);
    }

    public boolean isSuccess() {
        return "SUCCESS".equals(status);
    }
}
