package org.example.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;

@Data
@AllArgsConstructor
@ToString
public class Account {
    private int id;
    private String firstName;
    private String lastName;
    private double balance;

    public Account(String firstName, String lastName, double balance) {
        this.firstName = firstName;
        this.lastName = lastName;
        this.balance = balance;
    }
}
