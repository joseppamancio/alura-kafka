package br.com.alura.ecommerce;

import com.google.gson.*;

import java.lang.reflect.Type;

public class MessageAdapter implements JsonSerializer<Message>, JsonDeserializer<Message> {
    @Override
    public JsonElement serialize(Message message, Type type, JsonSerializationContext context) {
        JsonObject obj = new JsonObject();
        obj.addProperty("type", message.getPayload().getClass().getName()); // Pega o tipo do obj que queremos serializar
        obj.add("payload", context.serialize(message.getPayload())); // Pega o payload
        obj.add("correlationId", context.serialize(message.getId())); // Pega o Id
        return obj;
    }
    @Override
    public Message deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext context) throws JsonParseException {
        var obj = jsonElement.getAsJsonObject(); //converte em um objeto
        var payloadType = obj.get("type").getAsString(); // pega o tipo, o nome da classe
        var correlationId = (CorrelationId) context.deserialize(obj.get("correlatoinId"), CorrelationId.class); // pega o id

        try {
            var payload = context.deserialize(obj.get("payload"), Class.forName(payloadType));
            return new Message(correlationId, payload); // devolve uma nova Message
        } catch (ClassNotFoundException e) {
            throw new JsonParseException(e);
        }
    }
}
