package org.sirix.service.json.shredder;

import org.brackit.xquery.atomic.Int;
import org.junit.Test;
import org.sirix.service.json.JsonNumber;

import java.math.BigInteger;

import static org.junit.Assert.assertEquals;
import static org.testng.AssertJUnit.assertTrue;


public final class JsonNumberTest {

    @Test
    public void testFloat() {
        Float f = Float.MAX_VALUE -1;
        String s = Float.toString(f);
        Number n = JsonNumber.stringToNumber(s);

        assertTrue(n instanceof Float );
    }


    @Test
    public void testLong(){
        Long l = Long.MAX_VALUE -1;
        String s = Long.toString(l);
        Number n = JsonNumber.stringToNumber(s);

        assertTrue(n  instanceof Long );
    }


    @Test
    public void testInteger(){
        Integer i = Integer.MAX_VALUE -1;
        String s = Integer.toString(i);
        Number n = JsonNumber.stringToNumber(s);

        assertTrue(n  instanceof Integer );
    }


    @Test
    public void testBigInteger(){
        BigInteger b = BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE);
        String s = b.toString();
        Number n = JsonNumber.stringToNumber(s);

        assertTrue(n  instanceof BigInteger );
    }

    @Test
    public void testException() {
        String s = ("1.0ae10");

        try {
            JsonNumber.stringToNumber(s);
        } catch(IllegalStateException e){
        assertTrue(true);
    }
    }

}