./gradlew build
./gradlew runDSL --info
./gradlew runProcessorAPI --info


docker-compose exec kafka bash
kafka-console-producer --bootstrap-server localhost:9092 --topic users