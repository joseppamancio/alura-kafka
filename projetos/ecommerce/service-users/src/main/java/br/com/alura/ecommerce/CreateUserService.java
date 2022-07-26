package br.com.alura.ecommerce;

import br.com.alura.ecommerce.consumer.ConsumerService;
import br.com.alura.ecommerce.consumer.ServiceRunner;
import br.com.alura.ecommerce.database.LocalDatabase;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.SQLException;
import java.util.UUID;

public class CreateUserService implements ConsumerService<Order> {

    private final LocalDatabase database;

    CreateUserService() throws SQLException {
        this.database = new LocalDatabase("users_database");
        this.database.createIfNotExists("create table if not exists users(" +
                "uuid varchar(200) primary key," +
                "email varchar(200))");
    }

    public static void main(String[] args) {
        new ServiceRunner<>(CreateUserService::new).start(1); //permite a executar o serviço com multithread
    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_NEW_ORDER";
    }

    @Override
    public String getConsumerGroup() {
        return CreateUserService.class.getSimpleName();
    }

    public void parse(ConsumerRecord<String, Message<Order>> record) throws SQLException {
        System.out.println("---------------------------------------------");
        System.out.println("Processando new order, checking for new user");
        var message = record.value();
        System.out.println(message.getPayload());
        var order = message.getPayload();
        if(isNewUser(order.getEmail())){
            insertNewUser(order.getEmail());
        }
    }

    private void insertNewUser(String email) throws SQLException {
        var uuid = UUID.randomUUID().toString();
        database.update("insert into users(uuid, email) values('" + uuid + "', '" + email + "')");
        System.out.println("Usuário uuid "+uuid+" e email " +email+ " adicionado");
    }

    private boolean isNewUser(String email) throws SQLException {
        var results = database.query("select uuid from Users " +
                "where email = ? limit 1", email);
        return !results.next(); //Se tiver próxima linha é pq o usuário existe.
    }
}
