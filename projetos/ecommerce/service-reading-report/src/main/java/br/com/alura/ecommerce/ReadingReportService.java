package br.com.alura.ecommerce;

import br.com.alura.ecommerce.consumer.ConsumerService;
import br.com.alura.ecommerce.consumer.ServiceRunner;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

public class ReadingReportService implements ConsumerService<User> { //Consome Users

    private static final Path SOURCE = new File("src/main/resources/report.txt").toPath();

    public static void main(String[] args) {
        new ServiceRunner(ReadingReportService::new).start(5); //permite a executar o serviço com multithread
    }

    public void parse(ConsumerRecord<String, Message<User>> record) throws IOException {
        System.out.println("---------------------------------------------");
        var message = record.value();
        var user = message.getPayload();
        System.out.println("Processing report for " + user);

        var target = new File(user.getReportPath());  // path destino, classe user gera um nome
        IO.copyTo(SOURCE, target); // copia target de um lugar para outro
        IO.append(target, "Created for " + user.getUuid());

        System.out.println("File created: " + target.getAbsolutePath());
    }

    @Override
    public String getConsumerGroup() {
        return ReadingReportService.class.getSimpleName();
    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_USER_GENERATE_READING_REPORT";
    }
}
