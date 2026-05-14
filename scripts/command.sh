#!/bin/bash
# Prefect v3: deploy all flows from prefect.yaml
export CI_COMMIT_BRANCH=develop
prefect deploy --all