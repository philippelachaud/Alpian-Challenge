FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /app
COPY .mvn/ .mvn/
COPY mvnw mvnw.cmd pom.xml ./
RUN ./mvnw dependency:go-offline -B
COPY src ./src
RUN ./mvnw clean package -DskipTests -B
FROM eclipse-temurin:21-jre-jammy
WORKDIR /app
RUN groupadd -r processor && useradd -r -g processor processor
COPY --from=build /app/target/processor-*.jar app.jar
RUN chown -R processor:processor /app
USER processor
EXPOSE 8081
HEALTHCHECK --interval=30s --timeout=3s --start-period=40s --retries=3 \
  CMD curl -f http://localhost:8081/actuator/health || exit 1
ENTRYPOINT ["java", "-jar", "app.jar"]
