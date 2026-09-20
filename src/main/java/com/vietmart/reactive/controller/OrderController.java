package com.vietmart.reactive.controller;

import com.vietmart.reactive.dto.OrderCheckResponse;
import com.vietmart.reactive.service.InventoryClientService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.util.Map;

@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
@Slf4j
public class OrderController {

    private final InventoryClientService inventoryClientService;

    @GetMapping("/check-stock")
    public Mono<ResponseEntity<OrderCheckResponse>> checkStock(
            @RequestParam(defaultValue = "ITEM-001") String itemCode,
            @RequestParam(defaultValue = "false") boolean fail,
            @RequestParam(defaultValue = "0") long delayMs) {

        log.info("[REST CONTROLLER] Received reactive order check-stock request for item: {}, fail: {}, delayMs: {}",
                itemCode, fail, delayMs);

        return inventoryClientService.checkInventoryReactive(itemCode, fail, delayMs)
                .map(ResponseEntity::ok);
    }

    @GetMapping("/circuit-breaker-status")
    public Mono<ResponseEntity<Map<String, Object>>> getCircuitBreakerStatus() {
        var cb = inventoryClientService.getCircuitBreaker();
        var metrics = cb.getMetrics();

        Map<String, Object> status = Map.of(
                "name", cb.getName(),
                "state", cb.getState().name(),
                "failureRate", metrics.getFailureRate(),
                "numberOfBufferedCalls", metrics.getNumberOfBufferedCalls(),
                "numberOfFailedCalls", metrics.getNumberOfFailedCalls(),
                "numberOfSuccessfulCalls", metrics.getNumberOfSuccessfulCalls(),
                "numberOfNotPermittedCalls", metrics.getNumberOfNotPermittedCalls()
        );

        return Mono.just(ResponseEntity.ok(status));
    }
}
