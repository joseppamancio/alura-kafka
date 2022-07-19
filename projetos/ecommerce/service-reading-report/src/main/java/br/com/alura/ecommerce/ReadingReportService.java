package br.com.alura.ecommerce;

import br.com.alura.ecommerce.consumer.KafkaService;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class ReadingReportService {

    private static final Path SOURCE = new File("src/main/resources/report.txt").toPath();

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var reportService = new ReadingReportService();
        try(var service = new KafkaService<>(ReadingReportService.class.getSimpleName(), //Processo de desserialização de Order
                "ECOMMERCE_USER_GENERATE_READING_REPORT",
                reportService::parse,
                Map.of())) { // Nesse service não temos propriedades extras então passamos um mapa vazio.
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Message<User>> record) throws IOException {
        System.out.println("---------------------------------------------");
        var message = record.value();
        var user = message.getPayload();
        System.out.println("Processing report for " + user);

        var target = new File(user.getReportPath());  // path destino, classe user gera um nome
        IO.copyTo(SOURCE, target); // copia target de um lugar para outro
        IO.append(target, "Created for " + user.getUuid());

        System.out.println("File created: " + target.getAbsolutePath());
    }
}
