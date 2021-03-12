FROM python:3.8-slim
ENV DEBIAN_FRONTEND=noninteractive
RUN apt update
RUN apt -qq -y install python3 python3-pip
COPY radCAD /golem/resource/radCAD
RUN bash -c 'pip3 install /golem/resource/radCAD'
RUN bash -c 'pip3 install -U pip && pip3 install cadcad wheel seaborn matplotlib scipy networkx dill numpy yapapi pandas nest_asyncio'
WORKDIR /golem/work
COPY radcad_remote_agent.py /golem/entrypoint/
VOLUME /golem/work /golem/output /golem/resource
