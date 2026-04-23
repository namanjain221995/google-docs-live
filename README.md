# Google Doc History Worker — Complete Setup Guide

## Project Structure

```
google-doc-history-worker/
├── meeting_start_worker.py      # Polls meeting-start queue, registers Google watch
├── google_change_worker.py      # Dynamic thread pool, writes doc.txt live
├── google_watch_manager.py      # Renews Google file watches every 6 hours
├── doc_finalizer.py             # Copies temp doc.txt → final Interview-Success path
├── setup_db.sql                 # PostgreSQL schema
├── requirements.txt             # Python dependencies
├── iam_ec2_inline_policy.json   # EC2 role IAM policy (paste into AWS Console)
├── services/
│   ├── meeting-start-worker.service
│   ├── google-change-worker.service
│   └── google-watch-manager.service
└── README.md
```

---

## PART 1 — Push Code to GitHub (do this on your local machine)

```bash
# 1. Create repo on GitHub (name it: google-doc-history-worker)
#    Go to github.com → New Repository → google-doc-history-worker → Private → Create

# 2. On your local machine, put all files in one folder
mkdir google-doc-history-worker
cd google-doc-history-worker

# Copy all your .py files, .sql, requirements.txt, iam policy, README here

# 3. Create .gitignore  (IMPORTANT — never commit secrets)
cat > .gitignore << 'EOF'
*.pyc
__pycache__/
.env
env
*.log
state/
*.db
google_drive_watch_state.json
EOF

# 4. Initialize and push
git init
git add .
git commit -m "Initial commit: google doc history worker"
git branch -M main
git remote add origin https://github.com/YOUR_USERNAME/google-doc-history-worker.git
git push -u origin main
```

---

## PART 2 — Launch EC2 Instance

### Instance Settings
| Setting | Value |
|---|---|
| Instance type | **t3.large** |
| OS | Amazon Linux 2023 |
| Storage | 30 GB gp3 |
| IAM Role | Attach role with iam_ec2_inline_policy.json |
| Security Group | Allow SSH (port 22) from your IP only |
| Key pair | Use existing or create new |

### Attach IAM Inline Policy
```
1. Go to AWS Console → IAM → Roles
2. Find the role attached to your EC2 (or create one: EC2 → Actions → Security → Modify IAM Role)
3. Click the role name
4. Click "Add permissions" → "Create inline policy"
5. Click "JSON" tab
6. Paste the full contents of iam_ec2_inline_policy.json
7. Name it: doc-history-worker-policy
8. Click "Create policy"
```

---

## PART 3 — Install Everything on New EC2 (run these commands via SSH)

### Step 1 — Connect to EC2
```bash
ssh -i your-key.pem ec2-user@YOUR_EC2_PUBLIC_IP
```

### Step 2 — Install System Packages
```bash
sudo dnf update -y
sudo dnf install -y git python3.11 python3.11-pip postgresql15-server postgresql15
```

### Step 3 — Set Up PostgreSQL
```bash
# Initialize database
sudo postgresql-setup --initdb

# Start and enable
sudo systemctl enable postgresql
sudo systemctl start postgresql

# Set postgres user password
sudo -u postgres psql -c "ALTER USER postgres PASSWORD 'CHANGE_THIS_PASSWORD';"

# Allow password auth (edit pg_hba.conf)
sudo sed -i \
  's/^local[[:space:]]*all[[:space:]]*postgres[[:space:]]*peer/local   all             postgres                                md5/' \
  /var/lib/pgsql/data/pg_hba.conf

sudo systemctl restart postgresql

# Verify postgres is running
sudo systemctl status postgresql
```

### Step 4 — Clone Your GitHub Repo
```bash
# Set up your home directory
mkdir -p /home/ec2-user/doc-ui-worker/{logs,state}
cd /home/ec2-user/doc-ui-worker

# Clone the repo (use Personal Access Token for private repo)
# Generate token at: github.com → Settings → Developer Settings → Personal Access Tokens
git clone https://YOUR_GITHUB_TOKEN@github.com/YOUR_USERNAME/google-doc-history-worker.git .

# Verify files are present
ls -la
```

### Step 5 — Run PostgreSQL Schema
```bash
PGPASSWORD=CHANGE_THIS_PASSWORD psql -U postgres -d postgres -c "CREATE DATABASE dochistory;"
PGPASSWORD=CHANGE_THIS_PASSWORD psql -U postgres -d dochistory -f setup_db.sql
```

### Step 6 — Install Python Dependencies
```bash
pip3.11 install -r requirements.txt
```

### Step 7 — Create Environment File
```bash
cat > /home/ec2-user/doc-ui-worker/env << 'EOF'
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
DB_PASS=CHANGE_THIS_PASSWORD
DB_PORT=5432
DELETE_TEMP_AFTER_FINALIZE=false
EOF

# Lock down permissions — no one else can read secrets
chmod 600 /home/ec2-user/doc-ui-worker/env
```

### Step 8 — Install Systemd Services
```bash
# Copy service files
sudo cp services/meeting-start-worker.service /etc/systemd/system/
sudo cp services/google-change-worker.service /etc/systemd/system/
sudo cp services/google-watch-manager.service /etc/systemd/system/

# Reload systemd and enable all
sudo systemctl daemon-reload
sudo systemctl enable meeting-start-worker google-change-worker google-watch-manager

# Start all
sudo systemctl start meeting-start-worker
sudo systemctl start google-change-worker
sudo systemctl start google-watch-manager
```

### Step 9 — Verify Everything is Running
```bash
# Check status of all 3 workers
sudo systemctl status meeting-start-worker
sudo systemctl status google-change-worker
sudo systemctl status google-watch-manager

# Live log tailing
sudo journalctl -u meeting-start-worker -f
sudo journalctl -u google-change-worker -f
sudo journalctl -u google-watch-manager -f

# Or check log files directly
tail -f /home/ec2-user/doc-ui-worker/logs/meeting_start_worker.log
tail -f /home/ec2-user/doc-ui-worker/logs/google_change_worker.log
tail -f /home/ec2-user/doc-ui-worker/logs/google_watch_manager.log
```

### Step 10 — Verify PostgreSQL DB
```bash
PGPASSWORD=CHANGE_THIS_PASSWORD psql -U postgres -d dochistory -c "SELECT * FROM tracked_docs LIMIT 5;"
PGPASSWORD=CHANGE_THIS_PASSWORD psql -U postgres -d dochistory -c "SELECT * FROM doc_watch_state LIMIT 5;"
```

---

## PART 4 — How doc.txt Flows to Final Interview-Success Path

### Temp Location (during interview):
```
s3://zoom-automation-bucket/temp/live-doc-history/<meeting_id>/doc.txt
s3://zoom-automation-bucket/temp/live-doc-history/<meeting_id>/images/
s3://zoom-automation-bucket/temp/live-doc-history/<meeting_id>/snapshots/
```

### Final Location (after recording pipeline completes):
```
s3://zoom-automation-bucket/Interview-Success/<Host>/<Year>/<Month>/<Candidate>/<Meeting_ID>/<Company>/<YYYY-MM-DD>/<Round>/<Time-IST>/docs/doc.txt
s3://zoom-automation-bucket/Interview-Success/.../<Time-IST>/docs/images/
s3://zoom-automation-bucket/Interview-Success/.../<Time-IST>/docs/snapshots/
```

### To trigger finalization manually (for testing):
```bash
cd /home/ec2-user/doc-ui-worker
source env
python3.11 doc_finalizer.py \
  --meeting-id 89423156782 \
  --final-prefix "Interview-Success/John_Doe/2026/April/Rahul_Sharma/89423156782/Infosys/2026-04-23/Round_1/10-04-AM-IST"
```

### To trigger finalization from Lambda (zoom-s3-salesforce-linker):
The Lambda needs network access to EC2 OR you call finalize via an SQS message.
Simplest approach: call `doc_finalizer.finalize_docs(meeting_id, final_prefix)` 
directly inside `zoom-s3-salesforce-linker` Lambda after it builds the final path.

---

## PART 5 — Updating Code After Changes

```bash
# On your local machine, after making changes:
git add .
git commit -m "your change description"
git push origin main

# On EC2 — pull latest and restart:
cd /home/ec2-user/doc-ui-worker
git pull origin main
sudo systemctl restart meeting-start-worker google-change-worker google-watch-manager

# Verify restart
sudo systemctl status meeting-start-worker
```

---

## PART 6 — Worker Behavior Summary

| Scenario | What Happens |
|---|---|
| Meeting starts | `meeting_start_worker` registers Google watch, creates temp S3 state |
| Doc edited | Google sends webhook → SQS → worker thread processes change → doc.txt updated |
| Same doc edited 10 times | Same thread handles all 10 changes (reused) |
| 200 docs active | 200 threads running in pool simultaneously |
| Doc not edited for 30 min | Worker thread exits, doc marked IDLE |
| Doc edited again after idle | New thread spawns instantly |
| Google watch expires (7 days) | `google_watch_manager` renews before expiry |
| Recording pipeline finishes | `doc_finalizer` copies doc.txt to final Interview-Success path |

---

## PART 7 — Troubleshooting

### Worker not picking up messages
```bash
# Check SQS queue has messages
aws sqs get-queue-attributes \
  --queue-url https://sqs.us-east-1.amazonaws.com/985100584614/zoom-meeting-start-queue \
  --attribute-names ApproximateNumberOfMessages

# Check IAM permissions
aws sts get-caller-identity
```

### Google watch not receiving changes
```bash
# Check watch state in DB
PGPASSWORD=CHANGE_THIS_PASSWORD psql -U postgres -d dochistory \
  -c "SELECT doc_id, watch_channel_id, watch_expiry FROM doc_watch_state;"

# Manually trigger watch manager renewal
cd /home/ec2-user/doc-ui-worker
source env
python3.11 -c "from google_watch_manager import run_renewal_cycle; run_renewal_cycle()"
```

### doc.txt not created
```bash
# Check tracked_docs table
PGPASSWORD=CHANGE_THIS_PASSWORD psql -U postgres -d dochistory \
  -c "SELECT meeting_id, doc_id, status, last_change_at FROM tracked_docs;"

# Check S3
aws s3 ls s3://zoom-automation-bucket/temp/live-doc-history/ --recursive
```
