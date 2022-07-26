package br.com.alura.ecommerce.consumer;

import br.com.alura.ecommerce.Message;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public interface ConsumerService<T> { // metodos padrão de um serviço de consumo
    void parse(ConsumerRecord< String, Message<T>> record) throws Exception;
    String getConsumerGroup();
    String getTopic();
    }
