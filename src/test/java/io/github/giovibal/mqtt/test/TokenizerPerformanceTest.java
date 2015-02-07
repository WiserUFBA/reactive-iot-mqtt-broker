package io.github.giovibal.mqtt.test;

import io.github.giovibal.mqtt.ConversionUtility;
import io.github.giovibal.mqtt.MQTTPacketTokenizer;
import io.github.giovibal.mqtt.MQTTTokenizer;

import java.util.Calendar;

/**
 * Created by iddas on 22/01/2015.
 */
public class TokenizerPerformanceTest {
    private static final int MAX_LOOP = 1000000;

    public static void main(String args[]) {
        {
            String hexData = "10:21:00:06:4D:51:49:73:64:70:03:02:00:00:00:13:69:64:64:61:73:2E:31:34:32:31:39:36:35:30:30:36:38:33:33";
            byte[] data = ConversionUtility.fromHexString(hexData);
            System.out.println("START TEST = " + data.length + " [B]");
            {
                MQTTPacketTokenizer tokenizer = new MQTTPacketTokenizer();
                tokenizer.registerListener(new MQTTPacketTokenizer.MqttTokenizerListener() {
                    @Override
                    public void onToken(byte[] token, boolean timeout) {
                        //System.out.println("Token = " + ConversionUtility.toHexString(token, ":"));
                    }

                    @Override
                    public void onError(Throwable e) {
                        e.printStackTrace();
                    }
                });

                Calendar start = Calendar.getInstance();
                for (int i = 0; i < MAX_LOOP; i++) {
                    tokenizer.process(data);
                }
                Calendar stop = Calendar.getInstance();
                long delta = stop.getTimeInMillis() - start.getTimeInMillis();
                System.out.println("TOT = " + delta + " [ms]" + " " +tokenizer.getClass());
            }
            {
                MQTTTokenizer tokenizer = new MQTTTokenizer();
                tokenizer.registerListener(new MQTTTokenizer.MqttTokenizerListener() {
                    @Override
                    public void onToken(byte[] token, boolean timeout) {
                        //System.out.println("Token = " + ConversionUtility.toHexString(token, ":"));
                    }
                });


                Calendar start = Calendar.getInstance();
                for (int i = 0; i < MAX_LOOP; i++) {
                    tokenizer.process(data);
                }
                Calendar stop = Calendar.getInstance();
                long delta = stop.getTimeInMillis() - start.getTimeInMillis();
                System.out.println("TOT = " + delta + " [ms]" + " " +tokenizer.getClass());
            }
        }
        {
            String hexData = "31:F7:01:00:34:2F:65:63:6F:73:61:76:6F:6E:61:2F:6A:7A:2F:64:61:74:61:2F:66:72:61:6D:65:2F:69:6E:70:75:74:2F:30:30:30:30:2D:31:30:30:30:30:30:30:30:30:30:30:30:38:30:30:63:7B:22:62:22:3A:22:55:31:4E:54:44:49:41:41:41:41:41:41:41:42:41:41:41:47:62:32:7A:30:4D:41:41:51:41:50:41:52:59:58:4D:42:47:43:41:56:4E:54:55:77:79:41:41:41:41:41:41:41:41:51:41:41:41:53:41:4D:38:6D:41:43:4D:41:41:41:63:65:6F:41:38:42:46:68:63:77:4F:77:45:41:41:41:46:75:62:33:4D:41:43:67:41:55:41:42:34:41:2F:2F:39:69:49:78:43:41:39:41:45:41:41:41:41:54:76:48:38:3D:22:2C:22:74:22:3A:31:34:32:31:39:36:36:38:36:32:33:37:33:2C:22:73:22:3A:22:2F:31:32:37:2E:30:2E:30:2E:31:3A:35:30:31:38:38:22:2C:22:65:22:3A:22:30:30:30:30:2D:31:30:30:30:30:30:30:30:30:30:30:30:38:30:30:63:22:7D";
            byte[] data = ConversionUtility.fromHexString(hexData);
            System.out.println("START TEST = " + data.length + " [B]");
            {
                MQTTTokenizer tokenizer = new MQTTTokenizer();
                tokenizer.registerListener(new MQTTTokenizer.MqttTokenizerListener() {
                    @Override
                    public void onToken(byte[] token, boolean timeout) {
                        //System.out.println("Token = " + ConversionUtility.toHexString(token, ":"));
                    }
                });


                Calendar start = Calendar.getInstance();
                for (int i = 0; i < MAX_LOOP; i++) {
                    tokenizer.process(data);
                }
                Calendar stop = Calendar.getInstance();
                long delta = stop.getTimeInMillis() - start.getTimeInMillis();
                System.out.println("TOT = " + delta + " [ms]" + " " +tokenizer.getClass());
            }

            {
                MQTTPacketTokenizer tokenizer = new MQTTPacketTokenizer();
                tokenizer.registerListener(new MQTTPacketTokenizer.MqttTokenizerListener() {
                    @Override
                    public void onToken(byte[] token, boolean timeout) {
                        //System.out.println("Token = " + ConversionUtility.toHexString(token, ":"));
                    }

                    @Override
                    public void onError(Throwable e) {
                        e.printStackTrace();
                    }
                });


                Calendar start = Calendar.getInstance();
                for (int i = 0; i < MAX_LOOP; i++) {
                    tokenizer.process(data);
                }
                Calendar stop = Calendar.getInstance();
                long delta = stop.getTimeInMillis() - start.getTimeInMillis();
                System.out.println("TOT = " + delta + " [ms]" + " " +tokenizer.getClass());
            }
        }
    }

}
