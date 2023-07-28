FROM amazonlinux

RUN yum update -y

RUN yum install python3

RUN yum -y install python3-pip

RUN pip install boto3

RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"

RUN yum -y install unzip

RUN unzip awscliv2.zip

RUN ./aws/install

CMD [ "python", "./script.py" ]