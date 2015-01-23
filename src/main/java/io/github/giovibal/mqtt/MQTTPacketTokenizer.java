package io.github.giovibal.mqtt;

import java.nio.ByteBuffer;
import java.util.LinkedHashSet;

/**
 * Created by Paolo Iddas.
 * MQTT Protocol tokenizer.
 */
public class MQTTPacketTokenizer {
    public static void main(String[] args) {
        MQTTPacketTokenizer tokenizer = new MQTTPacketTokenizer();
        tokenizer.registerListener(new MQTTPacketTokenizer.MqttTokenizerListener() {
            @Override
            public void onToken(byte[] token, boolean timeout) {
                System.out.println("Token = " + ConversionUtility.toHexString(token, ":"));
            }
        });

        // byte[] data = new byte[]{0x00, 0x01};
        // byte[] data = new byte[]{0x00, 0x7f};
        // byte[] data = new byte[]{0x00, (byte)0x80, (byte)0x01};
        // byte[] data = new byte[]{0x00, (byte)0xFF, (byte)0x7f};
        // byte[] data = new byte[]{0x00, (byte)0x80, (byte)0x80, (byte)0x01};
        // byte[] data = new byte[]{0x00, (byte)0xFF, (byte)0xFF, (byte)0x7f};
        // byte[] data = new byte[]{0x00, (byte)0x80, (byte)0x80, (byte)0x80, (byte)0x01};
        // byte[] data = new byte[]{ 0x00, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0x7f };

        // byte[] data = new byte[] { 0x00, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x01 };
        // byte[] data = new byte[] { 0x00, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0x7f };

        {
            // byte[] data = new byte[]{0x00, 0x01, 0x30};

            byte[] data = new byte[]{0x00, 0x02, 0x30, 0x38};

            System.out.println("Processing " + ConversionUtility.toHexString(data, ":") + "...");
            MqttTokenizerState state = tokenizer.process(data);
            System.out.println("...State = " + state);
            tokenizer.debugState();
            System.out.println();
        }
        {
            {
                byte[] data = new byte[]{0x00};
                System.out.println("Processing " + ConversionUtility.toHexString(data, ":") + "...");
                MqttTokenizerState state = tokenizer.process(data);
                System.out.println("...State = " + state);
                tokenizer.debugState();
                System.out.println();
            }
            {
                byte[] data = new byte[]{0x05};
                System.out.println("Processing " + ConversionUtility.toHexString(data, ":") + "...");
                MqttTokenizerState state = tokenizer.process(data);
                System.out.println("...State = " + state);
                tokenizer.debugState();
                System.out.println();
            }
            {
                byte[] data = new byte[]{0x30, 0x37, 0x38};
                System.out.println("Processing " + ConversionUtility.toHexString(data, ":") + "...");
                MqttTokenizerState state = tokenizer.process(data);
                System.out.println("...State = " + state);
                tokenizer.debugState();
                System.out.println();
            }
            {
                byte[] data = new byte[]{0x34, 0x36, 0x00, 0x03};
                System.out.println("Processing " + ConversionUtility.toHexString(data, ":") + "...");
                MqttTokenizerState state = tokenizer.process(data);
                System.out.println("...State = " + state);
                tokenizer.debugState();
                System.out.println();
            }
            {
                byte[] data = new byte[]{0x30, 0x37, 0x38};
                System.out.println("Processing " + ConversionUtility.toHexString(data, ":") + "...");
                MqttTokenizerState state = tokenizer.process(data);
                System.out.println("...State = " + state);
                tokenizer.debugState();
                System.out.println();
            }
//            {
//                byte[] data = new byte[]{0x00};
//                MqttTokenizerState state = tokenizer.process(data);
//                System.out.println("State = " + state);
//            }

            {
                // byte[] data = new byte[]{0x00, 0x01, 0x30};

                byte[] data = new byte[]{ 0x00, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0x7f };

                System.out.println("Processing " + ConversionUtility.toHexString(data, ":") + "...");
                MqttTokenizerState state = tokenizer.process(data);
                System.out.println("...State = " + state);
                tokenizer.debugState();
                System.out.println();
            }
        }
    }

    /* tokenizer state machine */
    public static enum MqttTokenizerState {
        WAITING_FIRST, WAITING_REMAINING_LENGTH, WAITING_PAYLOAD
    }

    /* */
    private MqttTokenizerState tokenizerState = MqttTokenizerState.WAITING_FIRST;

    public MqttTokenizerState getTokenizerState() {
        return tokenizerState;
    }

    /* */
    private final static int MAX_FIXED_HEADER_LENGTH = 5;
    private ByteBuffer fixedHeader = ByteBuffer.allocate(MAX_FIXED_HEADER_LENGTH);
    private int fixedHeaderLength = 0;
    /* */
    private int multiplier = 1;
    private int remainingTotalLength = 0;
    private int remainingCounter = 0;
    /* */
    private ByteBuffer tokenContainer;

    /* listener */
    public interface MqttTokenizerListener {
        public void onToken(byte[] token, boolean timeout);
    }

    private LinkedHashSet<MqttTokenizerListener> listenerCollection = new LinkedHashSet<MqttTokenizerListener>();

    public MQTTPacketTokenizer() {
        init();
    }

    public void debugState() {
        System.out.println("STATE = " + getTokenizerState());
        System.out.println("FIXED HEADER LENGTH = " + fixedHeaderLength);
        System.out.println("PAYLOAD POSITION = " + remainingCounter);
        System.out.println("PAYLOAD LENGTH = " + remainingTotalLength);

    }

    public void init() {
        tokenizerState = MqttTokenizerState.WAITING_FIRST;
        fixedHeader.clear();
        fixedHeaderLength = 0;

        multiplier = 1;
        remainingTotalLength = 0;
        remainingCounter = 0;
    }

    protected int process(byte data) {
//        System.out.println(tokenizerState + " " + ConversionUtility.getHex(data) + "...");
        switch (tokenizerState) {
            case WAITING_FIRST:

                tokenizerState = MqttTokenizerState.WAITING_REMAINING_LENGTH;
                fixedHeader.put(data);
                fixedHeaderLength++;
                break;

            case WAITING_REMAINING_LENGTH:
                tokenizerState = MqttTokenizerState.WAITING_REMAINING_LENGTH;
                fixedHeader.put(data);
                fixedHeaderLength++;
                /* check for continuation bit */
                remainingTotalLength += ((data & 127) * multiplier);
                multiplier *= 128;
//                System.out.println("remainingTotalLength => " + remainingTotalLength);
                if ((data & 128) == 0 || fixedHeaderLength == MAX_FIXED_HEADER_LENGTH) {
                    tokenizerState = MqttTokenizerState.WAITING_PAYLOAD;
                    tokenContainer = ByteBuffer.allocate(fixedHeaderLength + remainingTotalLength);
                    tokenContainer.put(fixedHeader.array(), 0, fixedHeader.position());
                    if (remainingTotalLength == 0) {
                        complete(false);
                        init();
                    }
                }

                break;
            case WAITING_PAYLOAD:
                tokenContainer.put(data);
                remainingCounter++;
                if (remainingCounter == remainingTotalLength) {
                    tokenizerState = MqttTokenizerState.WAITING_FIRST;
                    complete(false);
                    init();
                }
                break;
            default:
                break;
        }

        // System.out.println("..." + tokenizerState);

        return 1;
    }

    protected int process(byte[] data, int offset, int length) {
        int processed = -1;
//        System.out.println("... " + (offset + 1) + " / " + length + " = " + ConversionUtility.getHex(data[offset]));
        switch (tokenizerState) {
            case WAITING_FIRST:
                processed = process(data[offset]);
                break;
            case WAITING_REMAINING_LENGTH:
                processed = process(data[offset]);
                break;

            case WAITING_PAYLOAD:
                processed = length - offset;
                int remaining = remainingTotalLength - remainingCounter;
                processed = processed < remaining ? processed : remaining;

                // System.out.println("... toProcess = " + processed + " remaining " + remaining);

                if (remainingCounter + processed <= remainingTotalLength) {
                    tokenContainer.put(data, offset, processed);
                    remainingCounter += processed;

                    if (remainingCounter == remainingTotalLength) {
                        tokenizerState = MqttTokenizerState.WAITING_FIRST;
                        complete(false);
                        init();
                    }
                }
                break;
            default:
                break;
        }

        return processed;
    }

    public MqttTokenizerState process(byte[] data) {
        // System.out.println("Processing " + ConversionUtility.toHexString(data, ":") + "...");

        int processed = 0;
        while (processed < data.length) {
            int temp = process(data, processed, data.length);
            processed += temp;
            if (temp == -1) {
//                throw new Exception();
                break;

            }
        }
        return getTokenizerState();
    }

    public void registerListener(MqttTokenizerListener listener) {
        listenerCollection.add(listener);
    }

    public void removeListener(MqttTokenizerListener listener) {
        listenerCollection.remove(listener);
    }

    public void removeAllListeners() {
        listenerCollection.clear();
    }

    private void notifyListeners(byte[] token, boolean timeout) {

        for (MqttTokenizerListener l : listenerCollection) {
            try {
                // System.out.println("T = " + ConversionUtility.toHexString(token, ":"));

                l.onToken(token, timeout);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void complete(boolean timeout) {
        try {
            byte[] aByteArray = tokenContainer.array();
            this.notifyListeners(aByteArray, timeout);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
