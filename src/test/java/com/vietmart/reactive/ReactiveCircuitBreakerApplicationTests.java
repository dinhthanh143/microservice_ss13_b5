package com.vietmart.reactive;

import com.vietmart.reactive.dto.OrderCheckResponse;
import com.vietmart.reactive.service.InventoryClientService;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
class ReactiveCircuitBreakerApplicationTests {

    @Autowired
    private InventoryClientService inventoryClientService;

    @Autowired
    private CircuitBreaker inventoryCircuitBreaker;

    @Test
    @DisplayName("Test 1: Call successfully when backend is healthy without any blocking")
    void testSuccessfulCallNonBlocking() {
        inventoryCircuitBreaker.reset();

        Mono<OrderCheckResponse> responseMono = inventoryClientService.checkInventoryReactive("ITEM-TEST-1", false, 0);

        StepVerifier.create(responseMono)
                .assertNext(response -> {
                    assertThat(response.isAvailable()).isTrue();
                    assertThat(response.getQuantity()).isEqualTo(150);
                    assertThat(response.getDataSource()).isEqualTo("LIVE_INVENTORY_SERVICE");
                })
                .verifyComplete();
    }

    @Test
    @DisplayName("Test 2: CircuitBreaker triggers fallback gracefully on errors")
    void testFallbackOnErrorsNonBlocking() {
        inventoryCircuitBreaker.reset();

        Mono<OrderCheckResponse> responseMono = inventoryClientService.checkInventoryReactive("FAIL", true, 0);

        StepVerifier.create(responseMono)
                .assertNext(response -> {
                    assertThat(response.isAvailable()).isFalse();
                    assertThat(response.getDataSource()).contains("FALLBACK");
                })
                .verifyComplete();
    }
}
