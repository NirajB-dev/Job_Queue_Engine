#!/bin/bash
# Run this ONCE in Cloud Shell after the bootstrap script completes.
# Sets up Workload Identity Federation so GitHub Actions can deploy to GCP
# without storing any service account keys.
set -euo pipefail

PROJECT_ID="job-queue-engine"
PROJECT_NUMBER=$(gcloud projects describe $PROJECT_ID --format='value(projectNumber)')
GITHUB_REPO="NirajB-dev/Job_Queue_Engine"
POOL_NAME="github-actions-pool"
PROVIDER_NAME="github-provider"
SA_NAME="github-actions-sa"
SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

echo "================================================================"
echo " Setting up Workload Identity Federation"
echo " Project : $PROJECT_ID ($PROJECT_NUMBER)"
echo " Repo    : $GITHUB_REPO"
echo "================================================================"

# ── SERVICE ACCOUNT ───────────────────────────────────────────────────────────
echo "[1/5] Creating GitHub Actions service account..."
gcloud iam service-accounts create $SA_NAME \
  --display-name="GitHub Actions Deployer" --quiet 2>/dev/null || echo "    Already exists."

# Grant roles needed by the deploy pipeline
for ROLE in \
  roles/run.admin \
  roles/container.developer \
  roles/artifactregistry.writer \
  roles/iam.serviceAccountUser \
  roles/cloudsql.client \
  roles/secretmanager.secretAccessor; do
  gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="$ROLE" --quiet
done
echo "    Service account ready: $SA_EMAIL"

# ── WORKLOAD IDENTITY POOL ────────────────────────────────────────────────────
echo "[2/5] Creating Workload Identity Pool..."
gcloud iam workload-identity-pools create $POOL_NAME \
  --location=global \
  --display-name="GitHub Actions Pool" --quiet 2>/dev/null || echo "    Already exists."

POOL_ID=$(gcloud iam workload-identity-pools describe $POOL_NAME \
  --location=global --format='value(name)')
echo "    Pool: $POOL_ID"

# ── WORKLOAD IDENTITY PROVIDER ────────────────────────────────────────────────
echo "[3/5] Creating Workload Identity Provider..."
gcloud iam workload-identity-pools providers create-oidc $PROVIDER_NAME \
  --location=global \
  --workload-identity-pool=$POOL_NAME \
  --display-name="GitHub Provider" \
  --attribute-mapping="google.subject=assertion.sub,attribute.actor=assertion.actor,attribute.repository=assertion.repository" \
  --issuer-uri="https://token.actions.githubusercontent.com" --quiet 2>/dev/null || echo "    Already exists."

PROVIDER_ID=$(gcloud iam workload-identity-pools providers describe $PROVIDER_NAME \
  --location=global \
  --workload-identity-pool=$POOL_NAME \
  --format='value(name)')
echo "    Provider: $PROVIDER_ID"

# ── BIND POOL TO SERVICE ACCOUNT ─────────────────────────────────────────────
echo "[4/5] Binding pool to service account..."
gcloud iam service-accounts add-iam-policy-binding $SA_EMAIL \
  --role="roles/iam.workloadIdentityUser" \
  --member="principalSet://iam.googleapis.com/${POOL_ID}/attribute.repository/${GITHUB_REPO}" \
  --quiet
echo "    Binding complete."

# ── PRINT GITHUB SECRETS ──────────────────────────────────────────────────────
echo "[5/5] Done. Add these as GitHub Actions secrets:"
echo ""
echo "    Go to: https://github.com/${GITHUB_REPO}/settings/secrets/actions"
echo ""
echo "    Secret name        : WIF_PROVIDER"
echo "    Secret value       : ${PROVIDER_ID}"
echo ""
echo "    Secret name        : WIF_SERVICE_ACCOUNT"
echo "    Secret value       : ${SA_EMAIL}"
echo ""
echo "================================================================"
echo " Workload Identity Federation setup complete."
echo " No service account keys were created or stored."
echo "================================================================"
