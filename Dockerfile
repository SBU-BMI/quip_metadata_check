FROM python:3.7.5-slim
MAINTAINER SBU-BMI

RUN pip install numpy pandas

WORKDIR /root
COPY . /root/.
RUN  chmod 0755 slide_check_manifest slide_combine_manifest 
ENV PATH=.:$PATH

CMD ["slide_check_manifest"]
