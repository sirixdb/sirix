package org.sirix.service.json.shredder;

import org.junit.Test;
import org.sirix.service.json.JsonNumber;

import java.math.BigInteger;

import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;


public final class JsonNumberTest {

    @Test
    public void testFloat() {
        Float f = Float.MAX_VALUE -1;
        String s = Float.toString(f);
        Number n = JsonNumber.stringToNumber(s);

        assertTrue("Expected type is Float", n instanceof Float);
    }


    @Test
    public void testLong(){
        Long l = Long.MAX_VALUE -1;
        String s = Long.toString(l);
        Number n = JsonNumber.stringToNumber(s);

        assertTrue("Expected type is Long", n instanceof Long);
    }


    @Test
    public void testInteger(){
        Integer i = Integer.MAX_VALUE -1;
        String s = Integer.toString(i);
        Number n = JsonNumber.stringToNumber(s);

        assertTrue("Expected type is Integer", n instanceof Integer);
    }


    @Test
    public void testBigInteger(){
        BigInteger b = BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE);
        String s = b.toString();
        Number n = JsonNumber.stringToNumber(s);

        assertTrue("Expected type is BigInteger", n instanceof BigInteger);
    }

    @Test
    public void testException() {
        String s = ("1.0ae10");

        try {
            JsonNumber.stringToNumber(s);
            fail("Expected IllegalStateException to be thrown");
        } catch(IllegalStateException e){
             assertTrue(true);
    }
    }

}