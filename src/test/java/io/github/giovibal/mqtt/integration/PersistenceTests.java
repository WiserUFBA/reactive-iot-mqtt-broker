package io.github.giovibal.mqtt.integration;

import io.github.giovibal.mqtt.test.HAClient;
import org.junit.Test;

/**
 * Created by giovanni on 14/06/2014.
 */
public class PersistenceTests extends BaseTest {


    @Test
    public void testCleanSession() {
        try {
            long t1, t2, t3;
            t1 = System.currentTimeMillis();
            boolean allTestsOk = true;
            String uri;
            uri = "tcp://localhost:1883";
//        uri = "tcp://iot.eclipse.org:1883";
//        uri = "tcp://localhost:61613";
            HAClient hac = new HAClient(uri);
            hac.test(2);
            allTestsOk &= hac.allTestsPass;
            hac.test(2);
            allTestsOk &= hac.allTestsPass;
            hac.test(2);
            allTestsOk &= hac.allTestsPass;
            hac.test(1);
            allTestsOk &= hac.allTestsPass;
            hac.test(1);
            allTestsOk &= hac.allTestsPass;
            hac.test(1);
            allTestsOk &= hac.allTestsPass;

            System.out.println();
            System.out.println("------------------------------------------------");
            if (allTestsOk) {
                System.out.println("All tests pass correctly!");
                System.out.println("This broker support QoS 1 and 2 with cleanSession=false");
            } else {
                System.out.println("Some test failed...");
            }

            t2 = System.currentTimeMillis();
            t3 = t2 - t1;
            System.out.println("Time elapsed: " + t3 + " millis.");
            System.out.println("------------------------------------------------");
            System.out.println();

//            assertTrue(allTestsOk);
//            testComplete();
        }
        catch(Throwable e) {
//            fail(e.getMessage());
        }
    }

}
