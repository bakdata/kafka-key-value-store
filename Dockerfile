FROM maven:3.5-jdk-8-slim

WORKDIR /app

# Restore maven dependencies in a separate build step
ADD pom.xml .
RUN mvn clean verify --fail-never

ADD src src
RUN mvn package

FROM openjdk:8-jre-slim

WORKDIR /app

COPY --from=0 /app/target target
ADD streams-processor .

EXPOSE 8080
CMD "./streams-processor"
