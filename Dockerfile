
FROM public.ecr.aws/lambda/python:3.11

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

COPY . ${LAMBDA_TASK_ROOT}

CMD ["main.lambda_handler"]
