ARG AWS_LAMBDA_FUNCTION_HANDLER='myfunction.lambda_handler'
FROM public.ecr.aws/lambda/python:3.12

 
ENV PATH="/usr/local/go/bin:${PATH}"

ENV AWS_LAMBDA_FUNCTION_HANDLER=${AWS_LAMBDA_FUNCTION_HANDLER}
ENV _HANDLER=${AWS_LAMBDA_FUNCTION_HANDLER}
ADD aws-lambda-rie /usr/local/bin/aws-lambda-rie
ENTRYPOINT /lambda-entrypoint.sh myfunction.lambda_handler
WORKDIR /var/task
COPY ./ ./
#COPY aws-lambda-rie /aws-lambda/aws-lambda-rie
#COPY ./entry_script.sh /lambda-entrypoint.sh
#chmod +x /lambda-entrypoint.sh
RUN pip install -r requirements.txt
CMD [ "myfunction.lambda_handler" ]