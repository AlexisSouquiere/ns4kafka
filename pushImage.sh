#! /bin/sh
export VERSION=0.0.6
export ARTIFACTORY_DOCKER_PUSH=docker.artifactory.michelin.com

./gradlew :kustomize:build

docker login docker.artifactory.michelin.com

docker build kustomize/ --pull -t $ARTIFACTORY_DOCKER_PUSH/taining/kkustomize:$VERSION
docker push $ARTIFACTORY_DOCKER_PUSH/taining/kkustomize:$VERSION