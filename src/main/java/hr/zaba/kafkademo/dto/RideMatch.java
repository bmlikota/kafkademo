package hr.zaba.kafkademo.dto;

import lombok.*;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RideMatch {
    private RideRequest ride;
    private DriverLocation driver;
}
