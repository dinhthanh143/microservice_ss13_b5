package com.vietmart.reactive.service;

import com.vietmart.reactive.dto.InventoryCheckResponse;
import com.vietmart.reactive.dto.OrderCheckResponse;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.reactor.circuitbreaker.operator.CircuitBreakerOperator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.time.Duration;

@Service
@RequiredArgsConstructor
@Slf4j
public class InventoryClientService {

    private final WebClient webClient;
    private final CircuitBreaker inventoryCircuitBreaker;

    public Mono<OrderCheckResponse> checkInventoryReactive(String itemCode, boolean simulateError, long delayMs) {
        log.info("[ORDER CLIENT] Preparing non-blocking request for item: {}, simulateError: {}, delayMs: {}",
                itemCode, simulateError, delayMs);

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
                // 100% Non-blocking: Bọc luồng Reactive bằng CircuitBreakerOperator (không dùng AOP Annotation)
                .transformDeferred(CircuitBreakerOperator.of(inventoryCircuitBreaker))
                .map(inventory -> {
                    log.info("[ORDER CLIENT SUCCESS] Received inventory response: {}", inventory);
                    return OrderCheckResponse.builder()
                            .itemCode(inventory.getItemCode())
                            .available(inventory.isInStock())
                            .quantity(inventory.getQuantity())
                            .dataSource("LIVE_INVENTORY_SERVICE")
                            .circuitBreakerState(inventoryCircuitBreaker.getState().name())
                            .message("Kiem tra ton kho truc tiep tu Inventory Service thanh cong.")
                            .build();
                })
                // Fallback khi CircuitBreaker dang OPEN (CallNotPermittedException)
                .onErrorResume(CallNotPermittedException.class, ex -> {
                    log.warn("[CIRCUIT BREAKER OPEN] Cầu dao đang ngắt! Kích hoạt Fallback bảo vệ hệ thống. Reason: {}", ex.getMessage());
                    return Mono.just(OrderCheckResponse.builder()
                            .itemCode(itemCode)
                            .available(false)
                            .quantity(0)
                            .dataSource("FALLBACK_LOCAL_CACHE")
                            .circuitBreakerState(inventoryCircuitBreaker.getState().name())
                            .message("Hệ thống kho đang quá tải hoặc tạm dừng (Circuit Breaker OPEN). Trả về dữ liệu tạm thời để bảo vệ hệ thống.")
                            .build());
                })
                // Fallback cho các lỗi mạng / 5xx khác trong quá trình gọi
                .onErrorResume(Throwable.class, ex -> {
                    log.error("[ORDER CLIENT ERROR] Lỗi khi gọi Inventory Service: {}. Chuyển sang chế độ Fallback.", ex.getMessage());
                    return Mono.just(OrderCheckResponse.builder()
                            .itemCode(itemCode)
                            .available(false)
                            .quantity(0)
                            .dataSource("FALLBACK_ERROR_HANDLER")
                            .circuitBreakerState(inventoryCircuitBreaker.getState().name())
                            .message("Không thể kết nối tới Inventory Service (" + ex.getClass().getSimpleName() + "). Phản hồi an toàn từ Fallback.")
                            .build());
                });
    }

    public CircuitBreaker getCircuitBreaker() {
        return inventoryCircuitBreaker;
    }
}
