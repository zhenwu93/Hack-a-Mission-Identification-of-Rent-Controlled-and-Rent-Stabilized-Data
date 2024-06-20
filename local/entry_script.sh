#!/bin/sh
if [ -z "${AWS_LAMBDA_RUNTIME_API}" ]; then
  exec /usr/local/bin/aws-lambda-rie /usr/bin/npx aws-lambda-ric 'myfunction.lambda_handler'
else
  exec /usr/bin/npx aws-lambda-ric 'myfunction.lambda_handler'
fi