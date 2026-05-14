skipRemainingStages = false
pipeline {
    agent any

    environment {
        REGISTORY_URL          = "https://<YOUR_REGISTRY>   // [SCRUBBED]"
        IMAGE_NAME             = "<YOUR_REGISTRY>/<YOUR_PROJECT>/prefect_v3/ingest/ingest_aws_s3_livingos   // [SCRUBBED]"
        DOCKER_CREDENTIALS_ID  = 'artifacts-registory-file'
        NAME_REPO              = 'ingest_aws_s3_livingos'
        TAG                    = setGitBranchName()
        BRANCH_NAME            = getGitBranchName()
        PREFECT_API_URL        = "http://<PREFECT_SERVER_HOST>/api   // [SCRUBBED]"
    }

    stages {

        stage('Who am I') {
            steps { sh 'whoami' }
        }

        stage('Check') {
            steps {
                sh 'echo $JOB_NAME'
                sh 'echo $WORKSPACE'
            }
        }

        stage('Code Analysis with SonarQube') {
            when { anyOf { branch 'develop'; branch 'main'; tag "*" } }
            steps {
                withCredentials([string(credentialsId: 'SONAR_TOKEN', variable: 'SONAR_TOKEN')]) {
                    sh """
                        sonar-scanner \
                            -Dsonar.host.url=${SONAR_HOST_URL} \
                            -Dsonar.projectKey=${NAME_REPO} \
                            -Dsonar.branch.name=${BRANCH_NAME} \
                            -Dsonar.login=${SONAR_TOKEN}
                    """
                }
            }
        }

        stage('Set Tag Name') {
            when { anyOf { branch 'develop'; branch 'main' } }
            steps { sh 'echo ${IMAGE_NAME}:${TAG}' }
        }

        stage('Build') {
            when { anyOf { branch 'develop'; branch 'main' } }
            steps {
                withCredentials([
                    file(credentialsId: DOCKER_CREDENTIALS_ID, variable: 'GCR_KEY'),
                    string(credentialsId: 'NPM_TOKEN', variable: 'NPM_TOKEN')
                ]) {
                    sh "cat \$GCR_KEY | docker login -u _json_key --password-stdin ${REGISTORY_URL}"
                    sh "docker build --build-arg NPM_TOKEN=${NPM_TOKEN} -t ${IMAGE_NAME}:${TAG} ."
                }
            }
        }

        stage('Push') {
            when { anyOf { branch 'develop'; branch 'main' } }
            steps {
                withCredentials([file(credentialsId: DOCKER_CREDENTIALS_ID, variable: 'GCR_KEY')]) {
                    sh "cat \$GCR_KEY | docker login -u _json_key --password-stdin ${REGISTORY_URL}"
                    sh "docker push ${IMAGE_NAME}:${TAG}"
                }
            }
        }

        // CHANGE POINT (v1 → v3):
        // Old: prefect create project + prefect register --path ./flows
        // New: PREFECT_DEPLOY_MODE=1 python deploy.py  (Prefect v3 Python API)
        stage('Deploy Prefect Flows') {
            when { anyOf { branch 'develop'; branch 'main' } }
            steps {
                script {
                    sh """
                        export IMAGE_TAG=${IMAGE_NAME}:${TAG}
                        docker run --rm \
                            -e PREFECT_API_URL=${PREFECT_API_URL} \
                            -e CI_COMMIT_BRANCH=${BRANCH_NAME} \
                            -e IMAGE_TAG=${IMAGE_NAME}:${TAG} \
                            -e PREFECT_DEPLOY_MODE=1 \
                            ${IMAGE_NAME}:${TAG} \
                            bash -c "python deploy.py"
                    """
                }
            }
        }

        stage('Remove Images') {
            when { anyOf { branch 'develop'; branch 'main' } }
            steps { sh "docker rmi -f ${IMAGE_NAME}:${TAG}" }
        }
    }
}

def getGitBranchName() {
    def branch_name = GIT_BRANCH
    if (branch_name.contains("origin/")) {
        branch_name = branch_name.split("origin/")[1]
    }
    return branch_name
}

def setGitBranchName() {
    def branch_name = GIT_BRANCH
    def commitSha   = GIT_COMMIT.take(8)
    if (branch_name.contains("origin/")) {
        branch_name = branch_name.split("origin/")[1]
    }
    return "${branch_name}-${commitSha}"
}