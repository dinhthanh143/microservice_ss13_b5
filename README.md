
## 1. Bối Cảnh & Vấn Đề Kiến Trúc

### 1.1. Tại sao KHÔNG DÙNG `@CircuitBreaker` (AOP) trong WebFlux?
- Trong lập trình Reactive (Spring WebFlux + Project Reactor), luồng dữ liệu hoạt động theo mô hình **Non-blocking Event Loop** và bất đồng bộ (Asynchronous Streams).
- Cơ chế AOP `@CircuitBreaker` truyền thống chặn và wrap lời gọi hàm tại thời điểm gọi method (Execution Time), nhưng đối với Reactive Streams, method chỉ **trả về một Publisher (`Mono` hoặc `Flux`) ngay lập tức** chứ dữ liệu chưa thực sự được emit hay xử lý.
- Nếu dùng AOP thông thường:
  - Cầu dao không bắt được lỗi diễn ra bên trong pipeline bất đồng bộ sau đó.
  - Nguy cơ cản trở Event Loop Thread nếu can thiệp sai luồng.
- **Giải pháp chuẩn:** Sử dụng toán tử phản ứng chuyên dụng **`CircuitBreakerOperator.of(circuitBreaker)`** kết hợp với **`.transformDeferred(...)`**. Toán tử này đăng ký trực tiếp vào Reactive Stream Lifecycle (OnNext, OnError, OnComplete, OnCancel), đảm bảo **100% Non-blocking** và bảo vệ chính xác các luồng `Mono`/`Flux`.

---

## 2. Kiến Trúc & Cấu Trúc Luồng Reactive

```
[Client / Postman]
        │ (Non-blocking HTTP GET)
        ▼
[OrderController]
        │
        ▼
[InventoryClientService]
        │ 1. WebClient GET /api/inventory/check
        │ 2. .bodyToMono(InventoryCheckResponse.class)
        │ 3. .transformDeferred(CircuitBreakerOperator.of(circuitBreaker))  <--- [Resilience4j Cầu Dao]
        │ 4. .map(...)
        │ 5. .onErrorResume(CallNotPermittedException.class, ...)         <--- [Fallback khi cầu dao OPEN]
        │ 6. .onErrorResume(Throwable.class, ...)                         <--- [Fallback khi lỗi mạng/5xx]
        ▼
[OrderCheckResponse (Mono)] ───► Trả về Client tức thời (Zero blocking)
```

### Mã Nguồn Cốt Lõi (`InventoryClientService.java`)
```java
public Mono<OrderCheckResponse> checkInventoryReactive(String itemCode, boolean simulateError, long delayMs) {
    return webClient.get()
            .uri(uriBuilder -> uriBuilder
                    .path("/api/inventory/check")
                    .queryParam("itemCode", itemCode)
                    .queryParam("simulateError", simulateError)
                    .queryParam("delayMs", delayMs)
                    .build())
            .retrieve()
            .bodyToMono(InventoryCheckResponse.class)
            .timeout(Duration.ofSeconds(3))
            // Non-blocking Circuit Breaker Transformation
            .transformDeferred(CircuitBreakerOperator.of(inventoryCircuitBreaker))
            .map(inventory -> OrderCheckResponse.builder()
                    .itemCode(inventory.getItemCode())
                    .available(inventory.isInStock())
                    .quantity(inventory.getQuantity())
                    .dataSource("LIVE_INVENTORY_SERVICE")
                    .circuitBreakerState(inventoryCircuitBreaker.getState().name())
                    .message("Kiểm tra tồn kho thành công.")
                    .build())
            // Fallback khi Circuit Breaker ở trạng thái OPEN
            .onErrorResume(CallNotPermittedException.class, ex -> Mono.just(OrderCheckResponse.builder()
                    .itemCode(itemCode)
                    .available(false)
                    .quantity(0)
                    .dataSource("FALLBACK_LOCAL_CACHE")
                    .circuitBreakerState(inventoryCircuitBreaker.getState().name())
                    .message("Hệ thống kho tạm dừng (Circuit Breaker OPEN).")
                    .build()))
            // Fallback cho lỗi khác
            .onErrorResume(Throwable.class, ex -> Mono.just(OrderCheckResponse.builder()
                    .itemCode(itemCode)
                    .available(false)
                    .quantity(0)
                    .dataSource("FALLBACK_ERROR_HANDLER")
                    .circuitBreakerState(inventoryCircuitBreaker.getState().name())
                    .message("Lỗi kết nối: " + ex.getClass().getSimpleName())
                    .build()));
}
```

---

## 3. Cấu Hình YAML & Phơi Bày Actuator Metrics

### `src/main/resources/application.yml`
```yaml
server:
  port: 8080

spring:
  application:
    name: reactive-order-service

# Cấu hình Circuit Breaker
resilience4j:
  circuitbreaker:
    instances:
      inventoryCircuitBreaker:
        sliding-window-type: COUNT_BASED
        sliding-window-size: 6                          # Kích thước cửa sổ đánh giá: 6 request
        minimum-number-of-calls: 4                       # Cần tối thiểu 4 request để bắt đầu tính tỷ lệ lỗi
        failure-rate-threshold: 50.0                    # Ngưỡng lỗi 50% -> chuyển OPEN
        wait-duration-in-open-state: 10s                 # Thời gian OPEN trước khi sang HALF_OPEN
        permitted-number-of-calls-in-half-open-state: 2  # Cho phép 2 request thử nghiệm khi HALF_OPEN
        automatic-transition-from-open-to-half-open-enabled: true

# Phơi bày Actuator & Micrometer
management:
  endpoints:
    web:
      exposure:
        include: "*"
  endpoint:
    health:
      show-details: always
  health:
    circuitbreakers:
      enabled: true
```

---

## 4. Theo Dõi Metrics Qua Actuator & Grafana

### 4.1. Endpoint quan sát trạng thái Cầu Dao:
```http
GET http://localhost:8080/actuator/metrics/resilience4j.circuitbreaker.state
```

### 4.2. JSON Response Mẫu từ Actuator:
Khi trạng thái là **`CLOSED`** (Bình thường):
```json
{
  "name": "resilience4j.circuitbreaker.state",
  "description": "The state of the circuit breaker",
  "measurements": [
    {
      "statistic": "VALUE",
      "value": 1.0
    }
  ],
  "availableTags": [
    {
      "tag": "name",
      "values": ["inventoryCircuitBreaker"]
    },
    {
      "tag": "state",
      "values": ["closed", "half_open", "open", "disabled", "metrics_only", "forced_open"]
    }
  ]
}
```

Để lọc trực tiếp theo tag state:
* `GET http://localhost:8080/actuator/metrics/resilience4j.circuitbreaker.state?tag=name:inventoryCircuitBreaker&tag=state:closed` $\rightarrow$ `VALUE: 1.0` (Đang Closed)
* `GET http://localhost:8080/actuator/metrics/resilience4j.circuitbreaker.state?tag=name:inventoryCircuitBreaker&tag=state:open` $\rightarrow$ `VALUE: 0.0` (Chưa Open)

---

## 5. Hướng Dẫn Kiểm Thử Từng Bước (Step-by-Step Test Guide)

### Bước 1: Khởi động ứng dụng
```bash
./gradlew bootRun
```

### Bước 2: Kiểm tra khi hệ thống khỏe mạnh (Trạng thái `CLOSED`)
Gửi request thành công:
```bash
curl "http://localhost:8080/api/orders/check-stock?itemCode=SP001"
```
Response:
```json
{
  "itemCode": "SP001",
  "available": true,
  "quantity": 150,
  "dataSource": "LIVE_INVENTORY_SERVICE",
  "circuitBreakerState": "CLOSED",
  "message": "Kiểm tra tồn kho thành công."
}
```

### Bước 3: Kích hoạt lỗi liên tục để mở Cầu Dao (`OPEN`)
Gửi 4 request liên tiếp với `fail=true`:
```bash
curl "http://localhost:8080/api/orders/check-stock?itemCode=SP001&fail=true"
curl "http://localhost:8080/api/orders/check-stock?itemCode=SP001&fail=true"
curl "http://localhost:8080/api/orders/check-stock?itemCode=SP001&fail=true"
curl "http://localhost:8080/api/orders/check-stock?itemCode=SP001&fail=true"
```
Kết quả: Tỷ lệ lỗi đạt 100% (> 50%). Circuit Breaker nhảy sang **`OPEN`**!

### Bước 4: Kiểm tra trạng thái OPEN & Fallback bảo vệ
Gửi ngay request bình thường (`fail=false`):
```bash
curl "http://localhost:8080/api/orders/check-stock?itemCode=SP001"
```
Response trả về ngay lập tức từ Fallback mà không cần gửi sang Inventory Service:
```json
{
  "itemCode": "SP001",
  "available": false,
  "quantity": 0,
  "dataSource": "FALLBACK_LOCAL_CACHE",
  "circuitBreakerState": "OPEN",
  "message": "Hệ thống kho tạm dừng (Circuit Breaker OPEN)."
}
```

Kiểm tra Metrics Actuator:
```bash
curl "http://localhost:8080/actuator/metrics/resilience4j.circuitbreaker.state?tag=name:inventoryCircuitBreaker&tag=state:open"
```
`VALUE: 1.0` (Xác nhận trạng thái OPEN đang active).

---

## 6. Bảng Checklist Tự Đánh Giá (Self-Assessment)

| Yêu cầu kỹ thuật | Hiện thực | Đạt |
| :--- | :--- | :---: |
| **Không chứa lệnh `.block()`** | 100% luồng code dùng `Mono`, `transformDeferred`, `onErrorResume`. |  **ĐẠT** |
| **Dùng WebClient gọi `/api/inventory/check`** | Đã cấu hình và gọi thông qua `WebClient`. |  **ĐẠT** |
| **Không dùng Annotation `@CircuitBreaker`** | Sử dụng `CircuitBreakerOperator.of(circuitBreaker)`. |  **ĐẠT** |
| **resilience4j-micrometer & actuator** | Đã khai báo dependencies và phơi bày actuator endpoints. |  **ĐẠT** |
| **Metrics `resilience4j.circuitbreaker.state`** | Hiển thị chính xác `VALUE: 1.0` / `0.0` theo từng tag `state`. |  **ĐẠT** |
