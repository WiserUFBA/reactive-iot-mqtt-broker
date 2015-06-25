package io.github.giovibal.mqtt;


import io.vertx.core.logging.Logger;
import io.vertx.core.logging.impl.LoggerFactory;

/**
 * Created by giova_000 on 17/01/2015.
 */
public class Container {
    private static Logger logger;
    public static Logger logger() {
        if(logger == null) {
//            LogDelegateFactory f = new JULLogDelegateFactory();
//            LogDelegateFactory f = new SLF4JLogDelegateFactory();
//            logger = new Logger(f.createDelegate("mqtt-broker-log"));
            logger = LoggerFactory.getLogger("mqtt-broker-log");
        }
        return logger;
    }
}
