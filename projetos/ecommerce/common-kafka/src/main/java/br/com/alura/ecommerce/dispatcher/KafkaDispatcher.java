package br.com.alura.ecommerce.dispatcher;

import br.com.alura.ecommerce.CorrelationId;
import br.com.alura.ecommerce.Message;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaDispatcher<T> implements Closeable { //Cloaseable permite que porta seja encerrada
    private final KafkaProducer<String, Message<T>> producer; // Message é o Objeto com cabeçalho
    public KafkaDispatcher(){
        this.producer = new KafkaProducer<>(properties());
    }
    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.31.97:9092");  //Configurando Endereço do Server ou 172.22.0.3
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());   // Tranforma bites em String - Key
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer.class.getName()); // Tranforma bites em Gson - Value
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all"); //Aguarda todos os brokers sincronizarem para enviar retorno
        return properties;
    }
    public void send(String topic, String key, CorrelationId id, T payload) throws ExecutionException, InterruptedException {
        Future<RecordMetadata> future = sendAsync(topic, key, id, payload);
        future.get();
    }

    public Future<RecordMetadata> sendAsync(String topic, String key, CorrelationId id, T payload) {
        var value = new Message<>(id.continueWith("_" + topic), payload);
        var record = new ProducerRecord<>(topic, key, value);

        // Send retorna um Future, Callback com valores e exception que se diferente de null então apresenta o erro
        Callback callback = (data, ex) -> {
            if (ex != null) {
                ex.printStackTrace();
                return;
            }
            System.out.println("Sucesso enviando " + data.topic() + ":::partition " + data.partition() + "/ offset " + data.offset() + "/ timestamp " + data.timestamp());
        };
        return producer.send(record, callback);
    }

    @Override
    public void close(){
        producer.close();
    }
}
