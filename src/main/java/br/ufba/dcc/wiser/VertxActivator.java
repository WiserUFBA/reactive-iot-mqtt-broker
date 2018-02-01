package br.ufba.dcc.wiser;

import static br.ufba.dcc.wiser.util.Calable.executeWithTCCLSwitch;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;

import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;

public class VertxActivator implements BundleActivator {

    static BundleContext bc;

    private ServiceRegistration vertxRegistration;
    private ServiceRegistration ebRegistration;

    @Override
    public void start(BundleContext bc) throws Exception {
        System.out.println("Starting the bundle Vertx MQQT Server for IoT");

        Vertx vertx = executeWithTCCLSwitch(() -> Vertx.vertx());

         System.out.println("vertx " + vertx);

        
        vertxRegistration = bc.registerService(Vertx.class, vertx, null);

        System.out.println("Vert.x service registered");
        //  LOGGER.info("Vert.x service registered");

        ebRegistration = bc.registerService(EventBus.class, vertx.eventBus(), null);
        System.out.println("Vert.x Event Bus service registered");
        // LOGGER.info("Vert.x Event Bus service registered");

        // VertxActivator.bc = bc;
    }

    @Override
    public void stop(BundleContext bc) throws Exception {

        System.out.println("Stopping the bundle bundle Vertx MQQT Server for IoT");
        if (vertxRegistration != null) {
         //   vertxRegistration.unregister();
            vertxRegistration = null;
        }
        if (ebRegistration != null) {
      //      ebRegistration.unregister();
            ebRegistration = null;
        }
    }
}
