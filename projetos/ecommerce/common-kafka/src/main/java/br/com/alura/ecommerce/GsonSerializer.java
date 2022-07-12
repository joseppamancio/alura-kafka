package br.com.alura.ecommerce;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Serializer;

public class GsonSerializer<T> implements Serializer<T> { // Implementa o Serializer do Kafka
    private final Gson gson = new GsonBuilder().registerTypeAdapter(Message.class, new MessageAdapter()).create(); // informações de desserialização está dentro do messageAdapter
    @Override
    public byte[] serialize(String s, T object) { // Serializa o objeto T, no caso o gson
        return gson.toJson(object).getBytes(); // transforma objeto em bytes
    }
}
