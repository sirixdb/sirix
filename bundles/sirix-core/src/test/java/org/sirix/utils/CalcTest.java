package org.sirix.utils;


import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CalcTest {

    @Test
    public void compareUAsPrefix(){
        byte[] value1 = new byte[]{2, 2, 2,2};
        assertEquals("Null, which should be maximum value, is smaller that non null.", -1, Calc.compareUAsPrefix(value1, null));
        assertEquals("Null, which should be maximum value, is smaller that non null.", 1, Calc.compareUAsPrefix(null, value1));
        assertEquals("Both are the largest possible value and thus equal.", 0, Calc.compareUAsPrefix(null, null));
        byte[] value2 = new byte[]{1, 1, 1, 1};
        assertEquals("Comparison where the first value is larger than the second gave error.", 1, Calc.compareUAsPrefix(value1, value2));
        byte[] value3 = new byte[]{2, 2, 2, 2, 2};
        assertEquals("Comparison where one is longer than the other gave the incorrect result", 1, Calc.compareUAsPrefix(value3, value1));
    }

    @Test
    public void compareAsPrefix(){
        byte[] value1 = new byte[]{2, 2, 2,2};
        assertEquals("Null, which should be maximum value, is smaller that non null.", -1, Calc.compareUAsPrefix(value1, null));
        assertEquals("Null, which should be maximum value, is smaller that non null.", 1, Calc.compareUAsPrefix(null, value1));
        assertEquals("Both are the largest possible value and thus equal.", 0, Calc.compareUAsPrefix(null, null));
        byte[] value2 = new byte[]{1, 1, 1, 1};
        assertEquals("Comparison where the first value is larger than the second gave error.", 1, Calc.compareUAsPrefix(value1, value2));
        byte[] value3 = new byte[]{2, 2, 2, 2, 2};
        assertEquals("Comparison where one is longer than the other gave the incorrect result", 1, Calc.compareUAsPrefix(value3, value1));
    }
}
