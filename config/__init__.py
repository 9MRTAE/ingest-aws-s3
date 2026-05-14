"""
config/__init__.py
Loads environment-specific settings and bootstraps GCP / AWS credentials.

Secrets are fetched from GCP Secret Manager at runtime — no plaintext
credentials in code or environment variables.
"""
import os

import gcsfs
from google.cloud import secretmanager
from google.oauth2 import service_account

APP_ENV = os.getenv("CI_COMMIT_BRANCH", "develop")
IMAGE_TAG = os.getenv("IMAGE_TAG", "latest")

if APP_ENV == "main":
    from .production import *       # noqa: F401, F403
elif APP_ENV == "develop":
    from .development import *      # noqa: F401, F403
else:
    from .development import *      # noqa: F401, F403


# ---------------------------------------------------------------------------
# GCP Secret Manager helpers
# ---------------------------------------------------------------------------
def _fetch_secret(secret_id: str, project_id: str) -> str:
    """Return the latest version of a GCP Secret Manager secret as a string."""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("utf-8")


# ── GCP Service Account ──────────────────────────────────────────────────────
_sa_json = _fetch_secret(SECRET_SERVICE_ACCOUNT, SECRET_PROJECT_ID)  # noqa: F821

import json as _json
_sa_info = _json.loads(_sa_json)

CREDENTIAL = service_account.Credentials.from_service_account_info(
    _sa_info,
    scopes=STORAGE_SCOPES,  # noqa: F821
)
GCSFS = gcsfs.GCSFileSystem(project=GCP_PROJECT, token=CREDENTIAL)  # noqa: F821


# ── AWS Credentials (fetched from Secret Manager) ───────────────────────────
_aws_key_secret_id    = AWS_ACCESS_KEY_ID       # noqa: F821  (secret ID string from config)
_aws_secret_secret_id = AWS_SECRET_ACCESS_KEY   # noqa: F821

AWS_ACCESS_KEY_ID     = _fetch_secret(_aws_key_secret_id,    SECRET_PROJECT_ID)  # noqa: F821
AWS_SECRET_ACCESS_KEY = _fetch_secret(_aws_secret_secret_id, SECRET_PROJECT_ID)  # noqa: F821

# ── GitLab Credentials (fetched from Secret Manager) ─────────────────────
GITLAB_TOKEN = _fetch_secret("your-secret-gitlab-token"   # [SCRUBBED] replace with your Secret Manager key name for GitLab token, SECRET_PROJECT_ID)  # noqa: F821
GITLAB_URL   = _fetch_secret("your-secret-gitlab-url"   # [SCRUBBED] replace with your Secret Manager key name for GitLab URL, SECRET_PROJECT_ID)    # noqa: F821