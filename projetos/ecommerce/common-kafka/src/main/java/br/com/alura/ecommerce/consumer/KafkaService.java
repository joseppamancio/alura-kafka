package br.com.alura.ecommerce.consumer;

import br.com.alura.ecommerce.Message;
import br.com.alura.ecommerce.dispatcher.GsonSerializer;
import br.com.alura.ecommerce.dispatcher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

public class KafkaService<T> implements Closeable { //Cloaseable permite que porta seja encerrada
    private final KafkaConsumer<String, Message<T>> consumer;
    private final ConsumerFunction parse;

    public KafkaService(String groupId, String topic, ConsumerFunction<T> parse, Map<String, String> properties) { // Construtor com Topico
        this(parse, groupId, properties);
        consumer.subscribe(Collections.singletonList(topic));
    }

    public KafkaService(String groupId, Pattern topic, ConsumerFunction<T> parse, Map<String, String> properties) { //Construtor com Pattern
        this(parse, groupId, properties);
        consumer.subscribe(topic);
    }

    private KafkaService(ConsumerFunction<T> parse, String groupId, Map<String, String> properties) { // Construtor para inicializar apenas os dois campos
        this.parse = parse;
        this.consumer = new KafkaConsumer<>(getProperties(groupId, properties));
    }

    public void run() throws ExecutionException, InterruptedException {
        try(var deadLetter = new KafkaDispatcher<>()){
            while (true) { // mantém o serviço ouvindo
                var records = consumer.poll(Duration.ofMillis(100)); // Verifica a cada 100 milissegundos se há registros
                if (!records.isEmpty()) {
                    System.out.println("Encontei " + records.count() + " registros");
                    for (var record : records) {
                        try {
                            parse.consume(record); // Para cada recorde chama-se o parse
                        } catch (Exception e) {
                            e.printStackTrace();
                            var message = record.value();
                            deadLetter.send("ECOMMERCE_DEADLETTER", message.getId().toString(),
                                    message.getId().continueWith("DeadLetter"),
                                    new GsonSerializer().serialize("", message));
                        }
                    }
                }
            }
        }
    }

    private Properties getProperties(String groupId, Map<String, String> overrideProperties){
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.31.97:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); // DeserializadorString - convete de binário para string
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId); // Necessário dar nome para quem é o consumidor, com isso temos o nome da classe
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString()); // ID do Consumidor, quando há mais de consumidores por grupo
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"1"); // Commit de mensagens de 1 em 1
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.putAll(overrideProperties); // tudo que vem de Override vai para properties, podendo ser prorpiedades extras
        return properties;
    }

    @Override
    public void close() {
        consumer.close();
    }
}
