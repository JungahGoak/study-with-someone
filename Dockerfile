FROM eclipse-temurin:17-jre-alpine

# healthcheck에서 curl 사용
RUN apk add --no-cache curl

WORKDIR /app
COPY build/libs/*.jar app.jar

EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
