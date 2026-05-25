package com.devalere.eventdriven.saga.order.controller;

import com.devalere.eventdriven.saga.inventory.repository.InventoryRepository;
import com.devalere.eventdriven.saga.order.repository.OrderRepository;
import com.devalere.eventdriven.saga.payment.repository.PaymentRepository;
import com.devalere.eventdriven.saga.saga.model.SagaState;
import com.devalere.eventdriven.saga.saga.orchestrator.OrderSagaOrchestrator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
@Slf4j
public class SagaController {

    private final OrderSagaOrchestrator orchestrator;
    private final OrderRepository orderRepository;
    private final PaymentRepository paymentRepository;
    private final InventoryRepository inventoryRepository;

    // =============================================
    //  DEMARRER UNE SAGA
    // =============================================

    /**
     * Demarrer une saga pour une nouvelle commande.
     * Ex: POST /api/saga?customer=Devalere&product=MacBook&quantity=1&amount=2499.99
     */
    @PostMapping("/saga")
    public ResponseEntity<SagaState> startSaga(
            @RequestParam String customer,
            @RequestParam String product,
            @RequestParam(defaultValue = "1") int quantity,
            @RequestParam double amount) {

        SagaState saga = orchestrator.startSaga(customer, product, quantity, amount);
        return ResponseEntity.ok(saga);
    }

    /**
     * Demo : lance 3 sagas automatiquement.
     * - 1 happy path (tout OK)
     * - 1 echec paiement (montant > 10000)
     * - 1 echec stock (quantite > 100)
     */
    @PostMapping("/saga/demo")
    public ResponseEntity<Map<String, String>> demo() {
        log.info("=== DEMO : Lancement de 3 sagas ===");

        SagaState saga1 = orchestrator.startSaga("Devalere", "MacBook Pro", 1, 2499.99);
        SagaState saga2 = orchestrator.startSaga("Alice", "Serveur Rack", 2, 15000.00);
        SagaState saga3 = orchestrator.startSaga("Bob", "Clavier", 200, 49.99);

        Map<String, String> result = new HashMap<>();
        result.put("saga1_happy_path", saga1.getSagaId() + " - MacBook Pro 2499.99EUR (devrait reussir)");
        result.put("saga2_payment_fail", saga2.getSagaId() + " - Serveur 15000EUR (paiement refuse > 10000)");
        result.put("saga3_stock_fail", saga3.getSagaId() + " - 200 Claviers (stock insuffisant > 100)");
        result.put("message", "Consultez les logs et GET /api/sagas pour voir le resultat !");

        return ResponseEntity.ok(result);
    }

    // =============================================
    //  CONSULTER LES SAGAS
    // =============================================

    @GetMapping("/sagas")
    public ResponseEntity<?> getAllSagas() {
        return ResponseEntity.ok(orchestrator.getAllSagas());
    }

    @GetMapping("/sagas/{sagaId}")
    public ResponseEntity<?> getSaga(@PathVariable String sagaId) {
        SagaState saga = orchestrator.getSaga(sagaId);
        if (saga == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(saga);
    }

    // =============================================
    //  DEBUG : VOIR L'ETAT DES SERVICES
    // =============================================

    @GetMapping("/orders")
    public ResponseEntity<?> getOrders() {
        return ResponseEntity.ok(orderRepository.findAll());
    }

    @GetMapping("/payments")
    public ResponseEntity<?> getPayments() {
        return ResponseEntity.ok(paymentRepository.findAll());
    }

    @GetMapping("/inventory")
    public ResponseEntity<?> getInventory() {
        return ResponseEntity.ok(inventoryRepository.findAll());
    }

    /**
     * Vue d'ensemble : sagas + orders + payments + inventory
     */
    @GetMapping("/dashboard")
    public ResponseEntity<Map<String, Object>> dashboard() {
        Map<String, Object> dashboard = new HashMap<>();
        dashboard.put("sagas", orchestrator.getAllSagas());
        dashboard.put("orders", orderRepository.findAll());
        dashboard.put("payments", paymentRepository.findAll());
        dashboard.put("inventory", inventoryRepository.findAll());
        return ResponseEntity.ok(dashboard);
    }
}
