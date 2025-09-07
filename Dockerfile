FROM public.ecr.aws/lambda/python:3.11

# Copy requirements and install Python deps
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy app code
COPY . ${LAMBDA_TASK_ROOT}

CMD ["main.lambda_handler"]