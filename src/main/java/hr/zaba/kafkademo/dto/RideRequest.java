package hr.zaba.kafkademo.dto;

import lombok.*;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RideRequest {
    private String rideId;
    private String driverId;
    private double lat;
    private double lon;
}
