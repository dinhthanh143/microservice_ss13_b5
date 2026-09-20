package com.vietmart.reactive.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderCheckResponse {
    private String itemCode;
    private boolean available;
    private int quantity;
    private String dataSource;
    private String circuitBreakerState;
    private String message;
}
