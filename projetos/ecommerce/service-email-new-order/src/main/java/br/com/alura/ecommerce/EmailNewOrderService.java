package br.com.alura.ecommerce;

import br.com.alura.ecommerce.consumer.ConsumerService;
import br.com.alura.ecommerce.consumer.ServiceRunner;
import br.com.alura.ecommerce.dispatcher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.concurrent.ExecutionException;

public class EmailNewOrderService implements ConsumerService<Order> {

    public static void main(String[] args) {
        new ServiceRunner(EmailNewOrderService::new).start(1); //permite a executar o serviço com multithread
    }

    private final KafkaDispatcher<String> emailDispatcher = new KafkaDispatcher<>();
    public void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException {
        System.out.println("---------------------------------------------");
        System.out.println("Processando new order, preparing email");
        var message = record.value();
        System.out.println(message);

        var emailCode = "Thank you for your order! We are processing your order!";
        var order = message.getPayload();
        var id = message.getId().continueWith(EmailNewOrderService.class.getSimpleName());
        emailDispatcher.send("ECOMMERCE_SEND_EMAIL", order.getEmail(), id, emailCode);
    }
    @Override
    public String getConsumerGroup() {
        return EmailNewOrderService.class.getSimpleName();
    }
    // Escuta Topico - ECOMMERCE_NEW_ORDER
    @Override
    public String getTopic() {
        return "ECOMMERCE_NEW_ORDER";
    }
}
