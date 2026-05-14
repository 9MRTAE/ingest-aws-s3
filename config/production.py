### GCP ###
STORAGE_SCOPES        = ["https://www.googleapis.com/auth/devstorage.full_control"]
GCP_PROJECT           = "your-gcp-project-id"          # [SCRUBBED] replace with your GCP project ID
GCP_BUCKET            = "your-prd-datalake-bucket"                 # [SCRUBBED] replace with your GCS bucket name
GCP_BUCKET_PARQUET    = "gcp-storage-parquet"
GCP_BUCKET_AWS_COST   = "gcp-storage-aws-costs"
GCP_BUCKET_APPLICATION = "cloudcost"
GCP_REGION            = "asia-southeast1"
GCP_BUCKET_AWS        = "AWS"

### Secret Manager ###
SECRET_PROJECT_ID      = "your-gcp-project-id"         # [SCRUBBED] replace with your GCP project ID
SECRET_SERVICE_ACCOUNT = "your-secret-sa-key"          # [SCRUBBED] replace with your Secret Manager key name for GCP SA JSON

### AWS ###
S3_BUCKET             = "your-s3-bucket"               # [SCRUBBED] replace with your AWS S3 bucket name
AWS_ACCESS_KEY_ID     = "your-secret-aws-key-id"       # [SCRUBBED] replace with your Secret Manager key name for AWS access key ID
AWS_SECRET_ACCESS_KEY = "your-secret-aws-secret-key"   # [SCRUBBED] replace with your Secret Manager key name for AWS secret access key
