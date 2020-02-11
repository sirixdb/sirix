package org.sirix.utils;

import org.junit.Test;
import org.sirix.settings.Constants;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

public class TypedValueTest {

    @Test
    public void getBytesTest(){
        //GIVEN
        String s = "hej";
        byte[] b1 = s.getBytes(Constants.DEFAULT_ENCODING);

        byte[] b2 = TypedValue.getBytes(s);

        //should be true if the conversion has been carried out correctly
        assertTrue(Arrays.equals(b1,b2));

        s = "";
        b1 = s.getBytes(Constants.DEFAULT_ENCODING);

        b2 = TypedValue.getBytes(s);

        //should be true since we should get an empty byte array
        assertTrue(Arrays.equals(b1,b2));

        s = "&amp;";
        b1 = s.getBytes(Constants.DEFAULT_ENCODING);

        b2 = TypedValue.getBytes("&");

        //should be true according to our specific conversion constraints
        assertTrue(Arrays.equals(b1,b2));


        s = "&lt;";
        b1 = s.getBytes(Constants.DEFAULT_ENCODING);

        b2 = TypedValue.getBytes("<");
        
        //should be true according to our specific conversion constraints
        assertTrue(Arrays.equals(b1,b2));


    }
}
