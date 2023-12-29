package com.github.alexgaard.mirror.postgres.utils;

import java.util.ArrayList;
import java.util.List;

public class ArrayUtils {

    public static List<Integer> toIntList(Short[] nums) {
        List<Integer> list = new ArrayList<>(nums.length);

        for (Short s : nums) {
            list.add((int) s);
        }

        return list;
    }

}
