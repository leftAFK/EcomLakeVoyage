FROM flink:1.20.0-scala_2.12
RUN apt-get update -qq && \
    apt-get install -y -qq python3 python3-pip && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    pip3 install apache-flink==1.20.0 -i https://pypi.tuna.tsinghua.edu.cn/simple -q && \
    rm -rf /var/lib/apt/lists/*
ENV FLINK_PYTHON_PATH=/usr/bin/python3
