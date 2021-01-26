package com.lin.vo;

import lombok.Data;

/**
 * @author lzr
 * @date 2020-09-19 19:18:43
 * 窗口聚合结果样例类
 */
@Data
public class ItemViewCountVo {
    /**
     * 商品id
     */
    private Long itemId;
    /**
     * 窗口结束时间
     */
    private Long windowsEnd;
    /**
     * 统计结果
     */
    private Long count;

    public ItemViewCountVo(Long itemId, long windowsEnd, Long count) {
        this.itemId = itemId;
        this.windowsEnd = windowsEnd;
        this.count = count;
    }
}
