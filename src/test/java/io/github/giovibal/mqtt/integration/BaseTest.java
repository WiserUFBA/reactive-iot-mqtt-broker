package io.github.giovibal.mqtt.integration;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.AsyncResultHandler;
import org.vertx.testtools.TestVerticle;

import static org.vertx.testtools.VertxAssert.assertNotNull;
import static org.vertx.testtools.VertxAssert.assertTrue;

/**
 * Created by giovanni on 14/06/2014.
 */
public class BaseTest extends TestVerticle {
    @Override
    public void start() {
        // Make sure we call initialize() - this sets up the assert stuff so assert functionality works correctly
        initialize();
        // Deploy the module - the System property `vertx.modulename` will contain the name of the module so you
        // don't have to hardecode it in your tests
        container.deployModule(System.getProperty("vertx.modulename"), new AsyncResultHandler<String>() {
            @Override
            public void handle(AsyncResult<String> asyncResult) {
                // Deployment is asynchronous and this this handler will be called when it's complete (or failed)
                assertTrue(asyncResult.succeeded());
                assertNotNull("deploymentID should not be null", asyncResult.result());
                // If deployed correctly then start the tests!
                startTests();
            }
        });
    }
}
