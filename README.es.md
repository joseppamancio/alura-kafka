[English](README.md) | [Português](README.pt.md)

[![Java](https://img.shields.io/badge/Java-ED8B00?style=for-the-badge&logo=java&logoColor=white)](https://www.oracle.com/java/technologies/javase-jdk11-downloads.html)
[![Maven](https://img.shields.io/badge/Maven-C71A36?style=for-the-badge&logo=apache-maven&logoColor=white)](https://maven.apache.org/)
[![Kafka](https://img.shields.io/badge/Kafka-231F20?style=for-the-badge&logo=apache-kafka&logoColor=white)](https://kafka.apache.org/)

# Comercio electrónico con Kafka
## 📄 Descripción del Proyecto
Desarrollado durante el curso de Alura, este proyecto simula un sistema de comercio electrónico utilizando Apache Kafka como plataforma de mensajería. Implementado en Java, hace uso de Maven como gestor de dependencias.

## 📦 Estructura
La arquitectura tiene un estilo de aplicación como una colección de servicios que son:
* Altamente mantenibles y testeables
* Débilmente acoplados
* Independientes en la implementación
* Organizados en torno a las capacidades comerciales
* Capaces de ser desarrollados por un equipo pequeño.

## ⚙️ Tecnologías
- Java
- Maven
- Apache Kafka
- Docker

## 🚀 Cómo ejecutar
1. Clona el repositorio.
2. En el directorio "docker-kafka/custom-image" ejecuta los siguientes comandos:
   - `docker-compose up` para levantar el contenedor de Kafka
   - `./start-kafka.sh` para iniciar el servidor de Kafka
3. En el directorio "projetos/ecommerce" ejecuta el comando:
   - `mvn clean install` para instalar las dependencias
4. El proyecto tiene los siguientes servicios:
   - "EmailService" para simular el envío de correo electrónico.
   - "NewOrderService" para simular la creación de un nuevo pedido.
   - "HttpEcommerceService" para simular la comunicación con un servicio externo.
   - "LogService" para simular el registro de mensajes.
   - "NewOrderMain" para simular la creación de un nuevo pedido.
   - "ReadingReportService" para simular la lectura de informes.
   - "CreateUserService" para simular la creación de un nuevo usuario.
   - "BatchSendMessageService" para simular el envío de mensajes en lote.
5. Ejecuta los servicios deseados y observa el funcionamiento de Kafka.
