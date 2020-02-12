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

        //WHEN
        byte[] b1 = s.getBytes(Constants.DEFAULT_ENCODING);
        byte[] b2 = TypedValue.getBytes(s);

        //THEN
        assertTrue("should be true if the conversion has been carried out correctly",Arrays.equals(b1,b2));

        //GIVEN
        s = "";

        //WHEN
        b1 = s.getBytes(Constants.DEFAULT_ENCODING);
        b2 = TypedValue.getBytes(s);

        //THEN
        assertTrue("should be true since we should get an empty byte array",Arrays.equals(b1,b2));

        //GIVEN
        s = "&amp;";

        //WHEN
        b1 = s.getBytes(Constants.DEFAULT_ENCODING);
        b2 = TypedValue.getBytes("&");

        //THEN
        assertTrue("should be true according to our specific conversion constraints",Arrays.equals(b1,b2));

        //GIVEN
        s = "&lt;";

        //WHEN
        b1 = s.getBytes(Constants.DEFAULT_ENCODING);
        b2 = TypedValue.getBytes("<");
        
        //THEN
        assertTrue("should be true according to our specific conversion constraints",Arrays.equals(b1,b2));


    }
}
