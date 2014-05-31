package io.github.giovibal.mqtt;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.LinkedHashSet;

/**
 * Created by Paolo Iddas.
 * MQTT Protocol tokenizer.
 */
public class MQTTTokenizer {
	public static void main(String[] args) {
		// byte[] data = new byte[]{0x00, 0x01};
		// byte[] data = new byte[]{0x00, 0x7f};
		// byte[] data = new byte[]{0x00, (byte)0x80, (byte)0x01};
		// byte[] data = new byte[]{0x00, (byte)0xFF, (byte)0x7f};
		// byte[] data = new byte[]{0x00, (byte)0x80, (byte)0x80, (byte)0x01};
		// byte[] data = new byte[]{0x00, (byte)0xFF, (byte)0xFF, (byte)0x7f};
		// byte[] data = new byte[]{0x00, (byte)0x80, (byte)0x80, (byte)0x80, (byte)0x01};
		byte[] data = new byte[] { 0x00, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0x7f };

		// byte[] data = new byte[] { 0x00, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x01 };
		// byte[] data = new byte[] { 0x00, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0x7f };

		MQTTTokenizer tokenizer = new MQTTTokenizer();
		tokenizer.process(data);
	}

	/* tokenizer state machine */
	public static enum MqttTokenizerState {
		WAITING_FIRST, WAITING_REMAINING_LENGTH, WAITING_PAYLOAD
	}

	private MqttTokenizerState tokenizerState = MqttTokenizerState.WAITING_FIRST;

	private final static int MAX_REMAINING_LENGTH = 4;
	private int remainingLengthCounter = 0;
	private int mulltiplier = 1;
	private int payloadLength = 0;
	private int payloadCounter = 0;
	private final ByteArrayOutputStream tokenContainer = new ByteArrayOutputStream();

	/* listener */
	public interface MqttTokenizerListener {
		public void onToken(byte[] token, boolean timeout);
	}

	private LinkedHashSet<MqttTokenizerListener> listenerCollection = new LinkedHashSet<MqttTokenizerListener>();

	public MQTTTokenizer() {
		init();
	}

	public MqttTokenizerState process(byte data) {
		// System.out.println(tokenizerState + " " + ConversionUtility.getHex(data) + "...");
		switch (tokenizerState) {
			case WAITING_FIRST:
				tokenizerState = MqttTokenizerState.WAITING_REMAINING_LENGTH;
				tokenContainer.write(data);
				break;

			case WAITING_REMAINING_LENGTH:
				tokenizerState = MqttTokenizerState.WAITING_REMAINING_LENGTH;
				tokenContainer.write(data);
				/* check for continuation bit */
				payloadLength += ((data & 127) * mulltiplier);
				mulltiplier *= 128;
				remainingLengthCounter++;
//                System.out.println("payloadLength => " + payloadLength);
                if ((data & 128) == 0 || remainingLengthCounter == MAX_REMAINING_LENGTH) {
					tokenizerState = MqttTokenizerState.WAITING_PAYLOAD;
					if (payloadLength == 0) {
						complete(false);
						init();
					}
				}

				break;
			case WAITING_PAYLOAD:
				tokenContainer.write(data);
				payloadCounter++;
				if (payloadCounter == payloadLength) {
					tokenizerState = MqttTokenizerState.WAITING_FIRST;
					complete(false);
					init();
				}
				break;
			default:
				break;
		}

		// System.out.println("..." + tokenizerState);

		return tokenizerState;
	}

	public void init() {
		tokenizerState = MqttTokenizerState.WAITING_FIRST;

		remainingLengthCounter = 0;
		mulltiplier = 1;
		payloadLength = 0;
		payloadCounter = 0;

		try {
			tokenContainer.reset();
			tokenContainer.flush();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}

	public MqttTokenizerState process(byte[] data) {
		for (int i = 0; i < data.length; i++) {
			process(data[i]);
		}
		return tokenizerState;
	}

	public void registerListener(MqttTokenizerListener listener) {
		listenerCollection.add(listener);
	}

	public void removeListener(MqttTokenizerListener listener) {
		listenerCollection.remove(listener);
	}

	private void notifyListeners(byte[] token, boolean timeout) {

		for (MqttTokenizerListener aListener : listenerCollection) {
			try {
				aListener.onToken(token, timeout);
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private void complete(boolean timeout) {
		try {
			byte[] aByteArray = tokenContainer.toByteArray();
			this.notifyListeners(aByteArray, timeout);
		}
		catch (Exception e) {
			e.printStackTrace();
		}

	}
}
