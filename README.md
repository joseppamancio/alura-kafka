[Português](README.pt.md) | [Español](README.es.md)

[![Java](https://img.shields.io/badge/Java-ED8B00?style=for-the-badge&logo=java&logoColor=white)](https://www.oracle.com/java/technologies/javase-jdk11-downloads.html)
[![Maven](https://img.shields.io/badge/Maven-C71A36?style=for-the-badge&logo=apache-maven&logoColor=white)](https://maven.apache.org/)
[![Kafka](https://img.shields.io/badge/Kafka-231F20?style=for-the-badge&logo=apache-kafka&logoColor=white)](https://kafka.apache.org/)

# E-commerce with Kafka
## 📄 Project Description
Developed during the Alura course, this project simulates an e-commerce system using Apache Kafka as a messaging platform. Implemented in Java, it makes use of Maven as a dependency manager.

## 📦 Structure
The architecture follows an application style as a collection of services that are:
* Highly maintainable and testable
* Loosely coupled
* Independently deployable
* Organized around business capabilities
* Capable of being developed by a small team.

## ⚙️ Technologies
- Java
- Maven
- Apache Kafka
- Docker

## 🚀 How to Run
1. Clone the repository.
2. In the "docker-kafka/custom-image" directory, run the following commands:
   - `docker-compose up` to bring up the Kafka container
   - `./start-kafka.sh` to start the Kafka server
3. In the "projects/ecommerce" directory, run the command:
   - `mvn clean install` to install dependencies
4. The project consists of the following services:
   - "EmailService" to simulate email sending.
   - "NewOrderService" to simulate the creation of a new order.
   - "HttpEcommerceService" to simulate communication with an external service.
   - "LogService" to simulate message logging.
   - "NewOrderMain" to simulate the creation of a new order.
   - "ReadingReportService" to simulate reading reports.
   - "CreateUserService" to simulate creating a new user.
   - "BatchSendMessageService" to simulate sending batch messages.
5. Run the desired services and observe Kafka's operation.
