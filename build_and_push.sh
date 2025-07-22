#!/bin/bash

# === CONFIGURACIÓN ===
ACCOUNT_ID="109308348564"
REGION="eu-west-1"
REPOSITORY_NAME="kvs-consumer-rekognition"
IMAGE_TAG="latest"

# === ECR URI ===
ECR_URI="$ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/$REPOSITORY_NAME"

# === Login en ECR ===
aws ecr get-login-password --region $REGION | docker login --username AWS --password-stdin $ECR_URI

# === Construir imagen multiplataforma compatible con ECS ===
docker buildx build \
  --platform linux/amd64 \
  -t $ECR_URI:$IMAGE_TAG \
  --push .

echo "✅ Imagen $ECR_URI:$IMAGE_TAG subida correctamente"
