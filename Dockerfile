FROM python:3.7.4-slim
MAINTAINER SBU-BMI

RUN pip install numpy pandas

WORKDIR /root
COPY . /root/.
RUN  chmod 0755 run_metadata_check
ENV PATH=.:$PATH

CMD ["run_metadata_check"]
