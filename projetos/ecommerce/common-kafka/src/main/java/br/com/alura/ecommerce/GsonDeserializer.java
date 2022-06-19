package br.com.alura.ecommerce;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class GsonDeserializer<T> implements Deserializer<T> {
    public static final String TYPE_CONFIG = "br.com.alura.ecommerce.type_config"; // nome da classe que usaremos para fazer a deserializacao
    private final Gson gson = new GsonBuilder().create();
    private Class<T> type;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        String typeName = String.valueOf(configs.get(TYPE_CONFIG));
        try {
            this.type = (Class<T>) Class.forName(typeName); // forçamos uma conversão para o tipo classe Te usamos this para usarmos fora do escopo
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Type fpr deserialization does not exist in the classpath.", e);
        }
    }

    @Override
    public T deserialize(String s, byte[] bytes) {
        return gson.fromJson(new String (bytes), type); // para deserializar é necesssário passar a classe, que usaremos com o nome de 'type'
    }
}
