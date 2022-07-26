package br.com.alura.ecommerce.consumer;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

public class ServiceProvider<T> implements Callable<Void> {

    private final ServiceFactory<T> factory;
    public ServiceProvider(ServiceFactory<T> factory) {
        this.factory = factory;
    }

    public Void call() throws Exception { // implementa uma factory que poderá ser instâncias varia vezes
        var myService = factory.create();
        try(var service = new KafkaService(myService.getConsumerGroup(),
                myService.getTopic(),
                myService::parse, //  emailService::parse -> methodReference, invoque essa função para cada record
                Map.of())) { // Nesse service não temos propriedades extras então passamos um mapa vazio.
            service.run();
        }
        return null;
    }
}
