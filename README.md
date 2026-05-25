# Event-Driven en 5 min - Video 11

## Saga Pattern avec Spring Boot + Kafka (Orchestration)

Projet de demonstration pour la serie YouTube **"Event-Driven en 5 min"** par [@Devalere](https://youtube.com/@Devalere).

---

### Architecture Saga Orchestration

```
Client                     Orchestrator                    Kafka                     Services
+--------+          +---------------------+          +-------------+          +------------------+
|  POST  |          | OrderSagaOrchestrator|          |             |          | OrderService     |
|  /api  |--------->|                     |          | saga-       |          | PaymentService   |
| /saga  |          | 1. Cree SagaState   |--------->| commands    |--------->| InventoryService |
|        |          | 2. Envoie commande  |          | (topic)     |          |                  |
+--------+          |                     |          +-------------+          | Chaque service:  |
                    |                     |                                   | - Execute ou     |
                    |                     |          +-------------+          |   compense       |
                    |                     |<---------| saga-       |<---------| - Repond SUCCESS |
                    | 3. Recoit reponse   |          | replies     |          |   ou FAILURE     |
                    | 4. Etape suivante   |          | (topic)     |          |                  |
                    |    ou compensation  |          +-------------+          +------------------+
                    +---------------------+
                             |
                             v
                    +------------------+
                    | saga_states (DB) |
                    | orders (DB)      |
                    | payments (DB)    |
                    | inventory (DB)   |
                    +------------------+
                         H2 Database
```

### Flux de la Saga

**Happy Path :**
```
T1: CREATE_ORDER   -> OrderService    -> SUCCESS -> orderId=1
T2: DEBIT_PAYMENT  -> PaymentService  -> SUCCESS -> payment debite
T3: RESERVE_STOCK  -> InventoryService-> SUCCESS -> stock reserve
T4: CONFIRM_ORDER  -> OrderService    -> SUCCESS -> commande confirmee
=> Saga COMPLETED
```

**Compensation (ex: stock insuffisant) :**
```
T1: CREATE_ORDER   -> SUCCESS
T2: DEBIT_PAYMENT  -> SUCCESS
T3: RESERVE_STOCK  -> FAILURE (stock insuffisant)
C2: COMPENSATE DEBIT_PAYMENT  -> rembourser
C1: COMPENSATE CREATE_ORDER   -> annuler commande
=> Saga FAILED (etat coherent)
```

### Lancer le projet

```bash
# 1. Demarrer Kafka (KRaft, sans Zookeeper)
docker-compose up -d

# 2. Lancer l'application
./mvnw spring-boot:run
```

### Tester

**1. Demo (3 sagas automatiques) :**
```bash
curl -X POST http://localhost:8080/api/saga/demo
```
Lance 3 sagas :
- MacBook Pro 2499.99 EUR (happy path)
- Serveur 15000 EUR (echec paiement > 10000)
- 200 Claviers (echec stock > 100)

**2. Creer une saga manuellement :**
```bash
curl -X POST "http://localhost:8080/api/saga?customer=Devalere&product=MacBook&quantity=1&amount=2499.99"
```

**3. Forcer un echec paiement :**
```bash
curl -X POST "http://localhost:8080/api/saga?customer=Alice&product=Serveur&quantity=1&amount=15000"
```

**4. Forcer un echec stock :**
```bash
curl -X POST "http://localhost:8080/api/saga?customer=Bob&product=Clavier&quantity=200&amount=49.99"
```

**5. Voir l'etat des sagas :**
```bash
curl http://localhost:8080/api/sagas
```

**6. Dashboard complet :**
```bash
curl http://localhost:8080/api/dashboard
```

**7. Debug par service :**
```bash
curl http://localhost:8080/api/orders
curl http://localhost:8080/api/payments
curl http://localhost:8080/api/inventory
```

### Points cles dans les logs

```
[ORCHESTRATOR] Saga abc12345 demarree pour Devalere - MacBook Pro x1 (2499.99EUR)
[ORCHESTRATOR] Commande envoyee -> topic=saga-commands, service=ORDER, action=EXECUTE, step=CREATE_ORDER
[ORDER-SERVICE] Commande #1 creee pour Devalere - MacBook Pro (2499.99EUR)
[REPLY] Reponse recue : saga=abc12345, service=ORDER, step=CREATE_ORDER, status=SUCCESS
[ORCHESTRATOR] Saga abc12345 - Etape CREATE_ORDER reussie
[ORCHESTRATOR] Commande envoyee -> service=PAYMENT, action=EXECUTE, step=DEBIT_PAYMENT
[PAYMENT-SERVICE] Paiement debite : 2499.99EUR pour commande #1
[ORCHESTRATOR] Saga abc12345 - Etape DEBIT_PAYMENT reussie
...
[ORCHESTRATOR] === SAGA abc12345 TERMINEE AVEC SUCCES === Commande #1 confirmee !
```

**En cas d'echec :**
```
[INVENTORY-SERVICE] Rupture de stock ! Quantite demandee : 200
[ORCHESTRATOR] Saga xyz789 - Etape RESERVE_STOCK ECHOUEE ! Raison: Stock insuffisant
[ORCHESTRATOR] Lancement des compensations...
[PAYMENT-SERVICE] COMPENSATION : Paiement rembourse
[ORDER-SERVICE] COMPENSATION : Commande annulee
[ORCHESTRATOR] === SAGA xyz789 ECHOUEE (compensations terminees) ===
```

### Concepts demontres

- **Saga Orchestration** : un orchestrateur central coordonne les etapes
- **Saga State** : l'etat de la saga est persiste en base (reprise possible)
- **Compensation** : chaque etape a son action inverse (annuler, rembourser, relacher)
- **Command/Reply** : 2 topics Kafka (saga-commands + saga-replies)
- **Echec simule** : paiement > 10000 EUR, stock > 100 unites
- **Dashboard** : vue complete de l'etat de tous les services

### Stack

- Java 17+ / Spring Boot 3.2.5
- Spring Kafka (KafkaTemplate + @KafkaListener)
- Spring Data JPA / H2
- Docker Compose (Kafka KRaft, sans Zookeeper)

---

*Made with love by Devalere - @Devalere*
