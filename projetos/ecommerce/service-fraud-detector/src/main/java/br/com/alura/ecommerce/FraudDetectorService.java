package br.com.alura.ecommerce;

import br.com.alura.ecommerce.consumer.ConsumerService;
import br.com.alura.ecommerce.consumer.KafkaService;
import br.com.alura.ecommerce.consumer.ServiceRunner;
import br.com.alura.ecommerce.dispatcher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class FraudDetectorService implements ConsumerService<Order> {

    public static void main(String[] args) {
        new ServiceRunner(FraudDetectorService::new).start(1); //permite a executar o serviço com multithread
    }
    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();
    public void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException {
        System.out.println("---------------------------------------------");
        System.out.println("Processando new order, checking for fraud");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());

        var message = record.value();
        //simulando processamento
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Simulando recusa de pedido
        var order = message.getPayload();
        if(isFraud(order)){
            System.out.println("Order is a fraud!!! " + order);
            orderDispatcher.send("ECOMMERCE_ORDER_REJECTED", order.getEmail(),
                    message.getId().continueWith(FraudDetectorService.class.getSimpleName()),
                    order);
        }else {
            System.out.println("Approved: "+ order);
            orderDispatcher.send("ECOMMERCE_ORDER_APPROVED", order.getEmail(),
                    message.getId().continueWith(FraudDetectorService.class.getSimpleName()),
                    order);
        }
    }

    @Override
    public String getConsumerGroup() {
        return FraudDetectorService.class.getSimpleName();
    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_NEW_ORDER";
    }

    private boolean isFraud(Order order) {
        return order.getAmount().compareTo(new BigDecimal("4500")) >= 0;
    }
}
