node ('beaware-jenkins-slave') {
    stage('Download Latest') {
        git(url: 'https://github.com/beaware-project/crisis-classification.git', branch: 'master')
        sh 'git submodule init'
        sh 'git submodule update'
    }

    stage ('Build docker image') {
	    sh 'docker build -t beaware/crisis-classification:${BUILD_NUMBER} --pull=true .'
    }

    stage ('Push docker image') {
        withDockerRegistry([credentialsId: 'dockerhub-credentials']) {
		sh 'docker push beaware/crisis-classification:${BUILD_NUMBER}'
        }
    }

    stage ('Deploy') {
        sh ''' sed -i 's/IMAGE_TAG/'"$BUILD_NUMBER"'/g' kubernetes/deploy.yaml '''
        sh 'kubectl apply -f kubernetes/deploy.yaml -n prod --validate=false'
        sh 'kubectl apply -f kubernetes/svc.yaml -n prod --validate=false'
    }

    stage ('Print-deploy logs') {
        sh 'sleep 200'
        sh 'kubectl -n prod logs deploy/crisis-classification -c crisis-classification'
    }
		
}
