package hr.zaba.kafkademo.dto;

import lombok.*;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Order {
    private Long customerId;
    private String product;
    private Double amount;
}
