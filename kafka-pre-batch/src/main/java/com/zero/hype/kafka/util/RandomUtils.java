package com.zero.hype.kafka.util;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Utility class for generating random values.
 */
public class RandomUtils {
    /**
     * Returns true with a given probability (percent chance).
     *
     * @param percent The probability (0-100) with which to return true.
     * @return True if a randomly generated double is less than percent / 100.0, false otherwise.
     */
    public static boolean chance(double percent) {
        return ThreadLocalRandom.current().nextDouble() < percent / 100.0;
    }
}
