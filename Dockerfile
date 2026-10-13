
FROM maven:3.9.11-eclipse-temurin-17 AS builder

WORKDIR /workspace

# Install the shared Maven parent inside the Docker build
COPY gwatcho-maven-parent/pom.xml /tmp/gwatcho-maven-parent/pom.xml

RUN mvn -B -f /tmp/gwatcho-maven-parent/pom.xml install -N

# Copy and build only the delivery service
COPY gwatcho-delivery-service/pom.xml /workspace/gwatcho-delivery-service/pom.xml
COPY gwatcho-delivery-service/src /workspace/gwatcho-delivery-service/src

RUN mvn -B -f /workspace/gwatcho-delivery-service/pom.xml clean package -DskipTests

FROM eclipse-temurin:17-jre

WORKDIR /app

COPY --from=builder /workspace/gwatcho-delivery-service/target/*.jar app.jar

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "app.jar"]
