package com.lin.userbehavior;

import com.lin.cep.OrderResult;
import lombok.Data;

import java.util.List;

/**
 * @author lzr
 * @date 2020-09-19 19:20:45
 * 输入数据样例类
 */
@Data
public class UserBehavior {
    private Long userId;
    private Long itemId;
    private Integer typeId;
    private String behavior;
    private Long timeStamp;
    private List<OrderResult> orderResults;


    public UserBehavior(Long userId, Long itemId, int typeId, String behavior, Long timeStamp) {
        this.userId = userId;
        this.itemId = itemId;
        this.typeId = typeId;
        this.behavior = behavior;
        this.timeStamp =timeStamp;
    }

    public UserBehavior() {

    }
}
