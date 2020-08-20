package com.example.kafkatest;

import lombok.*;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Item {
    Integer key;
    String value;
}
