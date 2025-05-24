package com.zero.hype.kafka.util;

import java.util.concurrent.ThreadLocalRandom;

public class RandomUtils {
    public static boolean chance(double percent) {
        return ThreadLocalRandom.current().nextDouble() < percent / 100.0;
    }
}