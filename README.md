# mta-k8s-jobs

Kubernetes CronJobs that process MTA (NYC Transit) data.
Each job is a self-contained directory under `jobs/` with its own Dockerfile,
Kubernetes manifest, and documentation.

---

## Repository structure

```
mta-k8s-jobs/
├── jobs/
│   ├── ridership-processor/       # Daily: fetches ridership CSV, converts with DuckDB, writes to DynamoDB
│   │   ├── Dockerfile
│   │   ├── src/main.py
│   │   ├── cronjob.yaml
│   │   └── README.md
│   └── mta-service-processor/     # Daily: processes MTA service data
│       ├── Dockerfile
│       ├── src/main.py
│       └── cronjob.yaml
├── base/
│   ├── namespace.yaml             # data-jobs namespace (applied once)
│   ├── secrets.yaml.example       # Template for the r2-credentials Secret
│   └── aws-secrets.yaml.example   # Template for the aws-credentials Secret
├── argocd/
│   ├── app-of-apps.yaml           # Root Argo CD Application (points to argocd/apps/)
│   └── apps/
│       ├── base.yaml              # Syncs base/namespace.yaml
│       ├── ridership-processor.yaml
│       └── mta-service-processor.yaml
├── .github/
│   └── workflows/
│       └── deploy.yml             # CI: auto-detect changed job, build and push to GHCR
└── .gitignore
```

---

## Prerequisites

- `kubectl` configured to point at your cluster
- AWS credentials (for DynamoDB access)
- Argo CD installed on the cluster (for GitOps deployment)

---

## Deployment

This project uses **Argo CD** for GitOps-based deployment. The app-of-apps pattern
in `argocd/` automatically syncs base manifests and each job's `cronjob.yaml` from
the `main` branch.

**CI pipeline:** When changes are pushed to `jobs/**` on `main`, the GitHub Actions
workflow detects which job changed, builds its Docker image, and pushes it to
`ghcr.io/<owner>/<job-name>` tagged with `latest` and the commit SHA.

Since Argo CD watches this repo, updating a `cronjob.yaml` image tag and pushing
to `main` will automatically deploy the change to the cluster.

---

## Setting up secrets in the cluster

Secrets are applied manually (not managed by Argo CD).

### AWS credentials (for DynamoDB)

```bash
cp base/aws-secrets.yaml.example base/aws-secrets.yaml
# edit base/aws-secrets.yaml with your AWS credentials
kubectl apply -f base/aws-secrets.yaml
```

### R2 credentials (if needed)

```bash
cp base/secrets.yaml.example base/secrets.yaml
# edit base/secrets.yaml with your R2 credentials
kubectl apply -f base/secrets.yaml
```

All `*secrets.yaml` files are excluded from version control via `.gitignore` — never commit them.

---

## How to add a new job

1. Copy an existing job directory as a starting point:
   ```bash
   cp -r jobs/ridership-processor jobs/my-new-job
   ```

2. Update `jobs/my-new-job/src/main.py` with your pipeline logic.

3. Update `jobs/my-new-job/cronjob.yaml`:
   - Set `metadata.name` to your job name.
   - Adjust the `schedule`, resource limits, and image reference.

4. Create an Argo CD Application manifest at `argocd/apps/my-new-job.yaml`
   (copy an existing one and update the `path` and `metadata.name`).

5. Push to `main` — GitHub Actions will build and push the image, and Argo CD
   will deploy the CronJob.

---

## Manually triggering a CronJob

```bash
kubectl create job <job-name>-manual \
  --from=cronjob/<job-name> \
  -n data-jobs
```

Follow the logs:

```bash
kubectl logs -n data-jobs -l job-name=<job-name>-manual -f
```

---

## Inspecting CronJobs

```bash
# List all CronJobs in the namespace
kubectl get cronjobs -n data-jobs

# Show recent job runs
kubectl get jobs -n data-jobs

# Describe a specific CronJob
kubectl describe cronjob ridership-processor -n data-jobs

# View logs from the most recent pod
kubectl logs -n data-jobs -l app=ridership-processor --tail=100
```
