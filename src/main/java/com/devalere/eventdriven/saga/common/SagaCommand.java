package com.devalere.eventdriven.saga.common;

/**
 * Commande envoyee par l'orchestrateur vers un service.
 * Le service execute l'action (ou la compensation) et repond via SagaReply.
 */
public record SagaCommand(
        String sagaId,
        String targetService,  // "ORDER", "PAYMENT", "INVENTORY"
        String action,         // "EXECUTE" ou "COMPENSATE"
        String step,           // "CREATE_ORDER", "DEBIT_PAYMENT", "RESERVE_STOCK", "CONFIRM_ORDER"
        Long orderId,
        String customer,
        String product,
        int quantity,
        double amount
) {
}
