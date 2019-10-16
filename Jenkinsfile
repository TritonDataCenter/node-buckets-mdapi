pipeline {

    agent {
        label '!platform:true && image_ver:18.4.0 && pkgsrc_arch:x86_64 && pi:20151126T062538Z && jenkins_agent:2'
    }

    options {
        buildDiscarder(logRotator(numToKeepStr: '90'))
        timestamps()
    }

    tools {
        nodejs 'sdcnode-v6-zone64'
    }

    stages {
        stage('check') {
            steps{
                sh('make check')
            }
        }
    }
}
