package com.lin.cep;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author lzr
 * @date 2020-10-25 19:43:26
 */
@Data
@AllArgsConstructor
public class OrderResult {
    /**
     * 订单id
     */
    private Long orderId;
    /**
     * 结果信息
     */
    private String resultMsg;
}
