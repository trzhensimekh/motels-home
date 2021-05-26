pipeline {
    agent any

    stages {
        stage('Build') {
            steps {
                sh 'python -m py_compile venv/com.training.spark/constants.py venv/com.training.spark/MotelsHomeRecommendation_empty.py'
                stash(name: 'compiled-results', includes: 'venv/com.training.spark/*.py*')
            }
        }
        stage('Test') {
            steps {
                sh 'py.test --junit-xml test-reports/results.xml venv/com.training.spark/motels_home_tests.py'
            }
            post {
                always {
                    junit 'test-reports/results.xml'
                }
            }
        }
    }
}