#!/usr/bin/env bash
# Builds the auto-login image, pushes it to ECR and (optionally) points the Lambda at the new image.
#
#   ./build_push.sh <aws-profile> [lambda-function-name]
#
# First run creates the ECR repo if it doesn't exist. Leave out the function name until the Lambda
# has been created (the console needs an image in ECR before you can create it).
set -euo pipefail

PROFILE="${1:?usage: ./build_push.sh <aws-profile> [lambda-function-name]}"
FUNCTION="${2:-}"
REGION="${REGION:-ap-south-1}"
REPO="${REPO:-auto-login}"

cd "$(dirname "$0")"

ACCOUNT=$(aws sts get-caller-identity --profile "$PROFILE" --query Account --output text)
REGISTRY="$ACCOUNT.dkr.ecr.$REGION.amazonaws.com"
IMAGE="$REGISTRY/$REPO:latest"

aws ecr describe-repositories --profile "$PROFILE" --region "$REGION" --repository-names "$REPO" >/dev/null 2>&1 \
  || aws ecr create-repository --profile "$PROFILE" --region "$REGION" --repository-name "$REPO" >/dev/null

aws ecr get-login-password --profile "$PROFILE" --region "$REGION" \
  | docker login --username AWS --password-stdin "$REGISTRY"

# --provenance=false: Lambda rejects the multi-manifest image buildx produces by default.
docker buildx build --platform linux/arm64 --provenance=false -t "$IMAGE" --push .
echo "pushed $IMAGE"

if [ -n "$FUNCTION" ]; then
  aws lambda update-function-code --profile "$PROFILE" --region "$REGION" \
    --function-name "$FUNCTION" --image-uri "$IMAGE" >/dev/null
  echo "lambda $FUNCTION now on $IMAGE"
fi
