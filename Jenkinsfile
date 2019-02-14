@Library('jenkins-helpers@v0.1.12') _

def label = "cognite-data-fetcher-${UUID.randomUUID().toString()}"

podTemplate(
    label: label,
    annotations: [
            podAnnotation(key: "jenkins/build-url", value: env.BUILD_URL ?: ""),
            podAnnotation(key: "jenkins/github-pr-url", value: env.CHANGE_URL ?: ""),
    ],
    containers: [
        containerTemplate(name: 'python',
            image: 'eu.gcr.io/cognitedata/multi-python:9b8bde1',
            command: '/bin/cat -',
            resourceRequestCpu: '1000m',
            resourceRequestMemory: '800Mi',
            resourceLimitCpu: '1000m',
            resourceLimitMemory: '800Mi',
            ttyEnabled: true),
    ],
    volumes: [
        secretVolume(secretName: 'jenkins-docker-builder', mountPath: '/jenkins-docker-builder', readOnly: true),
        secretVolume(secretName: 'pypi-credentials', mountPath: '/pypi', readOnly: true),
        configMapVolume(configMapName: 'codecov-script-configmap', mountPath: '/codecov-script'),
    ],
    envVars: [
        secretEnvVar(key: 'COGNITE_API_KEY', secretName: 'ml-test-api-key', secretKey: 'testkey.txt'),
        secretEnvVar(key: 'CODECOV_TOKEN', secretName: 'codecov-token-cognite-data-fetcher', secretKey: 'token.txt'),
        // /codecov-script/upload-report.sh relies on the following
        // Jenkins and Github environment variables.
        envVar(key: 'BRANCH_NAME', value: env.BRANCH_NAME),
        envVar(key: 'BUILD_NUMBER', value: env.BUILD_NUMBER),
        envVar(key: 'BUILD_URL', value: env.BUILD_URL),
        envVar(key: 'CHANGE_ID', value: env.CHANGE_ID),
    ]) {
    node(label) {
        def gitCommit
        container('jnlp') {
            stage('Checkout') {
                checkout(scm)
                gitCommit = sh(returnStdout: true, script: 'git rev-parse --short HEAD').trim()
            }
        }
        container('python') {
            stage('Install pipenv') {
                sh("pip3 install pipenv")
            }
            stage('Install dependencies') {
                sh("pipenv sync --dev")
            }
            stage('Check code style') {
                sh("pipenv run black -l 120 --check .")
                sh("pipenv run isort -w 120 -m 3 -tc -rc --check-only .")
            }
            stage('Test and coverage report') {
                sh("pyenv local 3.5.5 3.6.6 system")
                sh("pipenv run tox -p all")
                junit(allowEmptyResults: true, testResults: '**/test-report.xml')
                summarizeTestResults()
            }
            stage('Upload coverage reports') {
                sh 'bash </codecov-script/upload-report.sh'
                step([$class: 'CoberturaPublisher', coberturaReportFile: 'coverage.xml'])
            }
            stage('Build') {
                sh("python3 setup.py sdist bdist_wheel")
            }

            def deployToPyPi = false

            if (deployToPyPi) {
                def pipVersion = sh(returnStdout: true, script: 'pipenv run yolk -V cognite-data-fetcher | sort -n | tail -1 | cut -d\\  -f 2').trim()
                def currentVersion = sh(returnStdout: true, script: 'sed -n -e "/^__version__/p" cognite/data_fetcher/__init__.py | cut -d\\" -f2').trim()

                println("This version: " + currentVersion)
                println("Latest pip version: " + pipVersion)
                if (env.BRANCH_NAME == 'master' && currentVersion != pipVersion) {
                    stage('Release') {
                        sh("pipenv run twine upload --config-file /pypi/.pypirc dist/*")
                    }
                }
            }
        }
    }
}