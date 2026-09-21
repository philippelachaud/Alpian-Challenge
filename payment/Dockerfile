FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /app
COPY .mvn/ .mvn/
COPY mvnw mvnw.cmd pom.xml ./
RUN ./mvnw dependency:go-offline -B
COPY src ./src
RUN ./mvnw clean package -DskipTests -B
FROM eclipse-temurin:21-jre-jammy
WORKDIR /app
RUN groupadd -r payment && useradd -r -g payment payment
COPY --from=build /app/target/payment-service-*.jar app.jar
RUN chown -R payment:payment /app
USER payment
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
