FROM openjdk:14-jdk-buster
RUN apt-get update && apt-get install -y zip
RUN curl -s https://get.sdkman.io | bash && bash -c '. "/root/.sdkman/bin/sdkman-init.sh" && sdk install gradle'
