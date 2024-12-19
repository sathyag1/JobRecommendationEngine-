# stage # 1
FROM python:3.9-slim-bullseye as base
RUN /usr/local/bin/python -m pip install -U pip
WORKDIR /opt/app
RUN /usr/local/bin/python -m pip install setuptools wheel
RUN pip config set global.no-cache-dir true
COPY ./requirements.txt /opt/app/requirements.txt
RUN pip wheel --no-cache-dir --no-deps --wheel-dir /opt/app/wheels -r /opt/app/requirements.txt

# stage # 2
FROM python:3.9-slim-bullseye
RUN /usr/local/bin/python -m pip install -U pip
RUN pip config set global.no-cache-dir true
RUN useradd -m appuser
RUN chown -R appuser:appuser /home/appuser
COPY --from=base --chown=appuser:appuser /opt/app/wheels /home/appuser/wheels/
WORKDIR /home/appuser
RUN pip install --no-cache /home/appuser/wheels/*
ENV PATH="/home/appuser/.local/bin:${PATH}"

# copy every content from the local file to the image
WORKDIR /home/appuser
COPY --chown=appuser:appuser ./jobrec_v2.py /home/appuser/
COPY --chown=appuser:appuser ./DataLayer.py /home/appuser/
COPY --chown=appuser:appuser ./CommonLayer.py /home/appuser/
COPY --chown=appuser:appuser ./BusinessLayer.py /home/appuser/
COPY --chown=appuser:appuser ./start.sh /home/appuser/
RUN chmod 755 /home/appuser/start.sh
