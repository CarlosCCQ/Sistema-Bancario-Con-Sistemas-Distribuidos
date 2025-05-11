FROM openjdk:21-jre-slim

COPY ./src /app
WORKDIR /app

RUN javac -d bin *.java

CMD ["java", "-cp", "bin", "ServidorCentral"]