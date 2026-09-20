package com.vietmart.reactive.controller;

import com.vietmart.reactive.dto.InventoryCheckResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;

import java.time.Duration;

@RestController
@RequestMapping("/api/inventory")
@Slf4j
public class InventoryMockController {

    @GetMapping("/check")
    public Mono<ResponseEntity<InventoryCheckResponse>> checkInventory(
            @RequestParam(defaultValue = "ITEM-001") String itemCode,
            @RequestParam(defaultValue = "false") boolean simulateError,
            @RequestParam(defaultValue = "0") long delayMs) {

        log.info("[INVENTORY SERVER] Checking item: {}, simulateError: {}, delayMs: {}", itemCode, simulateError, delayMs);

        Mono<ResponseEntity<InventoryCheckResponse>> responseMono = Mono.defer(() -> {
            if (simulateError || "FAIL".equalsIgnoreCase(itemCode)) {
                log.warn("[INVENTORY SERVER] Simulating 500 INTERNAL SERVER ERROR for item: {}", itemCode);
                return Mono.error(new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Inventory Service Unavailable / Out of Stock Failure"));
            }

            InventoryCheckResponse response = InventoryCheckResponse.builder()
                    .itemCode(itemCode)
                    .inStock(true)
                    .quantity(150)
                    .warehouse("KHO-TONG-HN-01")
                    .status("AVAILABLE")
                    .build();

            return Mono.just(ResponseEntity.ok(response));
        });

        if (delayMs > 0) {
            return responseMono.delayElement(Duration.ofMillis(delayMs));
        }

        return responseMono;
    }
}
