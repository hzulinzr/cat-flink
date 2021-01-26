package com.lin.userbehavior;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

/**
 * @author lzr
 * @date 2020-09-21 15:21:55
 */
public class MyWaterMark implements WatermarkGenerator<UserBehavior> {

    private final long maxOutOfOrderness = 3500;

    private long currentMaxTimestamp;

    @Override
    public void onEvent(UserBehavior userBehavior, long eventTimestamp, WatermarkOutput watermarkOutput) {
        currentMaxTimestamp = Math.max(currentMaxTimestamp, eventTimestamp);
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
        watermarkOutput.emitWatermark(new Watermark(currentMaxTimestamp - maxOutOfOrderness - 1));
    }
}
