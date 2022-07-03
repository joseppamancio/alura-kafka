package br.com.alura.ecommerce;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

class KafkaDispatcher<T> implements Closeable { //Cloaseable permite que porta seja encerrada
    private final KafkaProducer<String, T> producer;
    KafkaDispatcher(){
        this.producer = new KafkaProducer<>(properties());
    }
    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.31.97:9092");  //Configurando Endereço do Server ou 172.22.0.3
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());   // Tranforma bites em String - Key
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer.class.getName()); // Tranforma bites em Gson - Value
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all"); //Aguarda todos os brokers sincronizarem para enviar confirmação
        return properties;
    }
    void send(String topic, String key, T value) throws ExecutionException, InterruptedException {
        var record = new ProducerRecord<>(topic, key, value);
        // Send retorna um Future, Callback com valores e exception que se diferente de null então apresenta o erro
        Callback callback = (data, ex) -> {
            if (ex != null) {
                ex.printStackTrace();
                return;
            }
            System.out.println("Sucesso enviando " + data.topic() + ":::partition " + data.partition() + "/ offset " + data.offset() + "/ timestamp " + data.timestamp());
        };
        producer.send(record, callback).get();  // Enviando Mensagens
    }

    @Override
    public void close(){
        producer.close();
    }
}
