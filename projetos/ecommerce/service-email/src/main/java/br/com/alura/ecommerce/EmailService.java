package br.com.alura.ecommerce;

import br.com.alura.ecommerce.consumer.ConsumerService;
import br.com.alura.ecommerce.consumer.ServiceRunner;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class EmailService implements ConsumerService<String> { //Consome 'T' representando Strings
    public static void main(String[] args) {
        new ServiceRunner(EmailService::new).start(5); //permite a executar o serviço com multithread
    }
    public String getConsumerGroup(){
        return EmailService.class.getSimpleName();
    }
    public String getTopic(){
        return "ECOMMERCE_SEND_EMAIL";
    }

    // Função que será executada para cada registro
    public void parse(ConsumerRecord < String, Message<String>> record){
        System.out.println("---------------------------------------------");
        System.out.println("Send email");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());

        //simulando processamento
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Email sent");
    }
}
