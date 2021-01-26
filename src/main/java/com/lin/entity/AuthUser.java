package com.lin.entity;

import lombok.Data;

import java.io.Serializable;

/**
 * @author lzr
 * @date 2020-12-29 10:29:25
 */
@Data
public class AuthUser implements Serializable {
    public AuthUser(Long id, Long createTime, String name) {
        this.id = id;
        this.createTime = createTime;
        this.name = name;
    }

    /**
     * 用户id
     */
    private Long id;
    /**
     * 用户注册时间
     */
    private Long createTime;
    /**
     * 用户账号
     */
    private String name;
}
