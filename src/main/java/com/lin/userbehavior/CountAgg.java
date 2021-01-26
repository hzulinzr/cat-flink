package com.lin.userbehavior;

import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * @author lzr
 * @date 2020-09-20 14:35:59
 * 自定义预聚合函数
 */
class CountAgg implements AggregateFunction<UserBehavior, Long, Long> {

    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(UserBehavior userBehavior, Long aLong) {
        return aLong + 1;
    }

    @Override
    public Long getResult(Long aLong) {
        return aLong;
    }

    @Override
    public Long merge(Long aLong, Long acc1) {
        return acc1 + aLong;
    }
}