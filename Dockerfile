FROM asia-southeast1-docker.pkg.dev/infra-thelivingos/thelivingos/devops/docker-images/prefect:1.2.4-python3.9-Y2025
WORKDIR /app

# Install system deps + pip ก่อน (cache layer ดีขึ้น)
RUN apt-get update && apt-get install -y --no-install-recommends curl git \
    && rm -rf /var/lib/apt/lists/*
RUN curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py \
    && python get-pip.py \
    && rm -rf get-pip.py

# Copy requirements ก่อน → cache pip install layer
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code ทีหลัง → แก้ code ไม่ต้อง reinstall deps
COPY . .