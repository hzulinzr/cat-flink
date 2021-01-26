package com.lin.entity;

import com.lin.cep.OrderResult;
import lombok.Data;

import java.util.List;

/**
 * @author lzr
 * @date 2020-09-07 18:43:16
 */
@Data
public class User {
    private Long id;
    private String username;
    private String password;
    private Long currentTime;
    private List<OrderResult> orderResults;

    public User(long id, String username, String password, Long currentTime) {
        this.id = id;
        this.username = username;
        this.password = password;
        this.currentTime = currentTime;
    }

    public User(long parseLong, String s, String s1) {
    }
    public User() {
    }

}
