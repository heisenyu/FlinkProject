package com.xiao.chapter05;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.List;
import java.util.Random;

/**
 * @Author : Kuangjm
 * @Time : 2022/7/2 2:02
 * @Project : FlinkProject
 * @Version : 1.0
 */
public class ClickSource implements SourceFunction<Event> {
    private boolean isRunning = true;

    @Override
    public void run(SourceContext ctx) throws Exception {

        Random random = new Random();

        String[] names = {"Mary", "Alice", "Bob", "Cary"};
        String[] urls = {"./home", "./cart", "./fav", "./prod?id=1", "./prod?id=2"};

        while (isRunning) {
            ctx.collect(new Event(names[random.nextInt(names.length)], urls[random.nextInt(urls.length)], Calendar.getInstance().getTimeInMillis()));

            Thread.sleep(1000);
        }

    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
