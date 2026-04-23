#!/bin/bash
# =============================================================
# EC2 SETUP SCRIPT for google-doc-history worker
# Run as ec2-user on a fresh t3.large Amazon Linux 2023
# =============================================================

set -e

echo "=== Step 1: Install system packages ==="
sudo dnf update -y
sudo dnf install -y python3.11 python3.11-pip postgresql15-server postgresql15 git

echo "=== Step 2: Initialize PostgreSQL ==="
sudo postgresql-setup --initdb
sudo systemctl enable postgresql
sudo systemctl start postgresql

# Set postgres password and create DB + user
sudo -u postgres psql -c "ALTER USER postgres PASSWORD 'your_db_password_here';"
sudo -u postgres psql -c "CREATE DATABASE dochistory;"

# Allow local password auth
sudo sed -i 's/^local.*all.*postgres.*peer/local   all             postgres                                md5/' /var/lib/pgsql/data/pg_hba.conf
sudo systemctl restart postgresql

echo "=== Step 3: Create app directory ==="
mkdir -p /home/ec2-user/doc-ui-worker/{logs,state,scripts}

echo "=== Step 4: Copy source files ==="
# Copy your Python files here:
# meeting_start_worker.py
# google_change_worker.py
# google_watch_manager.py
# setup_db.sql

echo "=== Step 5: Run DB schema ==="
PGPASSWORD=your_db_password_here psql -U postgres -d dochistory -f /home/ec2-user/doc-ui-worker/scripts/setup_db.sql

echo "=== Step 6: Install Python dependencies ==="
pip3.11 install \
  boto3 \
  psycopg2-binary \
  simple-salesforce \
  google-auth \
  google-auth-httplib2 \
  google-api-python-client \
  requests \
  cryptography

echo "=== Step 7: Create environment file ==="
cat > /home/ec2-user/doc-ui-worker/env << 'ENV'
AWS_REGION=us-east-1
S3_BUCKET=zoom-automation-bucket
MEETING_START_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/985100584614/zoom-meeting-start-queue
CHANGE_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/985100584614/google-doc-change-queue
GOOGLE_WEBHOOK_URL=https://5cs3mviba8.execute-api.us-east-1.amazonaws.com/google-drive/webhook
SF_SECRET_NAME=sf/jwt/credentials
GOOGLE_SECRET_NAME=google/doc-history-service-account
DB_HOST=localhost
DB_NAME=dochistory
DB_USER=postgres
DB_PASS=your_db_password_here
DB_PORT=5432
ENV

chmod 600 /home/ec2-user/doc-ui-worker/env
echo "Environment file created."

echo "=== Step 8: Install systemd services ==="

# ─── meeting-start-worker.service ───
sudo tee /etc/systemd/system/meeting-start-worker.service > /dev/null << 'SERVICE'
[Unit]
Description=Zoom Meeting Start Worker - Docs Tracker
After=network.target postgresql.service
Requires=postgresql.service

[Service]
Type=simple
User=ec2-user
WorkingDirectory=/home/ec2-user/doc-ui-worker
EnvironmentFile=/home/ec2-user/doc-ui-worker/env
ExecStart=/usr/bin/python3.11 /home/ec2-user/doc-ui-worker/meeting_start_worker.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal
SyslogIdentifier=meeting-start-worker

[Install]
WantedBy=multi-user.target
SERVICE

# ─── google-change-worker.service ───
sudo tee /etc/systemd/system/google-change-worker.service > /dev/null << 'SERVICE'
[Unit]
Description=Google Doc Change Worker - Live History
After=network.target postgresql.service
Requires=postgresql.service

[Service]
Type=simple
User=ec2-user
WorkingDirectory=/home/ec2-user/doc-ui-worker
EnvironmentFile=/home/ec2-user/doc-ui-worker/env
ExecStart=/usr/bin/python3.11 /home/ec2-user/doc-ui-worker/google_change_worker.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal
SyslogIdentifier=google-change-worker
# Allow many threads for 200 concurrent docs
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
SERVICE

# ─── google-watch-manager.service ───
sudo tee /etc/systemd/system/google-watch-manager.service > /dev/null << 'SERVICE'
[Unit]
Description=Google Watch Manager - Renews file watches
After=network.target postgresql.service
Requires=postgresql.service

[Service]
Type=simple
User=ec2-user
WorkingDirectory=/home/ec2-user/doc-ui-worker
EnvironmentFile=/home/ec2-user/doc-ui-worker/env
ExecStart=/usr/bin/python3.11 /home/ec2-user/doc-ui-worker/google_watch_manager.py
Restart=always
RestartSec=30
StandardOutput=journal
StandardError=journal
SyslogIdentifier=google-watch-manager

[Install]
WantedBy=multi-user.target
SERVICE

echo "=== Step 9: Enable and start services ==="
sudo systemctl daemon-reload
sudo systemctl enable meeting-start-worker google-change-worker google-watch-manager
sudo systemctl start meeting-start-worker google-change-worker google-watch-manager

echo "=== Done! Check status with: ==="
echo "sudo systemctl status meeting-start-worker"
echo "sudo systemctl status google-change-worker"
echo "sudo systemctl status google-watch-manager"
echo ""
echo "=== View logs with: ==="
echo "sudo journalctl -u meeting-start-worker -f"
echo "sudo journalctl -u google-change-worker -f"
echo "sudo journalctl -u google-watch-manager -f"
