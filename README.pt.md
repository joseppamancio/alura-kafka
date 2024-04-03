[English](README.md) | [Español](README.es.md)

[![Java](https://img.shields.io/badge/Java-ED8B00?style=for-the-badge&logo=java&logoColor=white)](https://www.oracle.com/java/technologies/javase-jdk11-downloads.html)
[![Maven](https://img.shields.io/badge/Maven-C71A36?style=for-the-badge&logo=apache-maven&logoColor=white)](https://maven.apache.org/)
[![Kafka](https://img.shields.io/badge/Kafka-231F20?style=for-the-badge&logo=apache-kafka&logoColor=white)](https://kafka.apache.org/)

# E-commerce com Kafka
## 📄 Descrição do Projeto
Desenvolvido durante o curso da Alura, este projeto simula um sistema de e-commerce utilizando Apache Kafka como plataforma de mensageria. Implementado em Java, faz uso do Maven como gerenciador de dependências.

## 📦 Estrutura
A arquitetura possuium um estilo de uma aplicação como uma coleção de serviços que são:  
* Altamente mantíveis e testáveis
* Fracamente acoplados
* Independentes na implantação
* Organizados em torno das capacidades de negócios
* Capazes de serem desenvolvidos por uma pequena equipe.

## ⚙️ Tecnologias
- Java
- Maven
- Apache Kafka
- Docker

## 🚀 Como executar
1. Clone o repositório
2. No diretorio "docker-kafka/custom-image" execute os seguinte comandos:
    - `docker-compose up` para subir o container do Kafka
    - `./start-kafka.sh` para subir o servidor do Kafka
3. No diretorio "projetos/ecommerce" execute o comando:
    - `mvn clean install` para instalar as dependências
4. O projeto possui os seguintes serviços:
    - "EmailService" para simular o envio de email.
    - "NewOrderService" para simular a criação de um novo pedido.
    - "HttpEcommerceService" para simular a comunicação com um serviço externo.
    - "LogService" para simular o log de mensagens.
    - "NewOrderMain" para simular a criação de um novo pedido.
    - "ReadingReportService" para simular a leitura de relatórios.
    - "CreateUserService" para simular a criação de um novo usuário.
    - "BatchSendMessageService" para simular o envio de mensagens em lote.
5. Execute os serviços desejados e observe o funcionamento do Kafka.


