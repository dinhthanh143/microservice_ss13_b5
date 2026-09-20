package com.vietmart.reactive.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class InventoryCheckResponse {
    private String itemCode;
    private boolean inStock;
    private int quantity;
    private String warehouse;
    private String status;
}
