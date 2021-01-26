package com.lin.cep;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author lzr
 * @date 2020-10-25 19:41:10
 */
@Data
@AllArgsConstructor
public class OrderEvent {
    /**
     * 订单id
     */
    private Long orderId;
    /**
     * 事件类型
     */
    private String eventType;
    /**
     * 交易id
     */
    private String txId;
    /**
     * 发生时间
     */
    private Long eventTime;

}
