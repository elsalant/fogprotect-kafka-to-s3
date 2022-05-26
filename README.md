### This is an example of Fybrik read module that uses REST protocol to connect to a FHIR server to obtain medical records.  Policies redact the information returned by the FHIR server or can even restrict access to a given resource type.
# User authentication is enabled, as well as (optional) logging to Kafka

Do once:  make sure helm v3.7+ is installed
> helm version

1. Install fybrik from the instructions in: https://fybrik.io/v0.6/get-started/quickstart/
2. Start the Kafka server:  
   - helm install kafka bitnami/kafka -n fybrik-system  
3. Create a namespace for the kafka-s3 demo:  
kubectl create namespace kafka-s3
4. Pull the files:
git pull https://github.com/elsalant/fogprotect-kafka-to-s3.git
5. Install the policy:  
\<ROOT>/scripts/applyPolicy.sh
6. Apply the FHIR server secrets and permissions  
\<ROOT>/sqlToFHIR/deployPermissions.sh 
7. kubectl edit cm cluster-metadata -n fybrik-system
and change theshire to UK
8. kubectl apply -f \<ROOT>/sqlToFHIR/asset.yaml
9. Apply the module
kubectl apply -f \<ROOT>/yaml/kafkaToS3module.yaml  
10. Apply the application - note that the name (or JWT) for the requester is in the label.requestedBy field!
kubectl apply -f \<ROOT>/yaml/kafakToS3application.yaml
11. Test
- a) Send events to the Kafka queue  
kubectl port-forward svc/kafka -n fybrik-system 9002:9002 
kafka-console-consumer --topic sm --from-beginning --bootstrap-server localhost:9092
{"DOB": "01/02/1988", "FirstName": "John", "LastNAME": "Jones"}
- b) Port-forward pod in fybrik-blueprints  
 kubectl get pods -n fybrik-blueprints  
eg: kubectl port-forward pod/\<POD ID> -n fybrik-blueprints 5559:5559

#### Hints
To test redaction: pick a field in the resource (e.g. "id") and set the tag in the asset.yaml file to "PII".
Note that to redact a given field in a given resource, e.g. "id" in "Patient" sources, in the asset.yaml file, specify the componentsMetadata value as "Patient.id".

If either the asset or policy is changed, then the Fybrik application needs to be restarted:
kubectl delete -f <name of FybrikApplication file>  
kubectl apply -f <name of FybrikApplication file>
 
#### DEVELOPMENT

1. To build Docker image:  
cd /Users/eliot/projects/HEIR/code/kafka-to-s3  
make docker-build  

Push the image to Docker package repo  
make docker-push

2. Push the Helm chart to the repo
export HELM_EXPERIMENTAL_OCI=1  
helm registry login -u elsalant -p \<PASSWORD> ghcr.io

Package the chart: (cd charts)
helm package kafka-to-s3 -d /tmp
helm push /tmp/kakfa-to-s3-chart-0.0.1.tgz oci://ghcr.io/elsalant

##### Development hints
1. files/conf.yaml controls the format of the policy evaluation.  This will be written into a file mounted inside the pod running in the fybrik-blueprints namespace.
2. templates/deployment.yaml defines the mount point (e.g. /etc/conf/conf.yaml) for this file.
3. Redaction values defined in values.yaml will be ignored.  This information will be supplied by the manager and connectors.
4. The FHIR server can be queried directly by:
 - kubectl port-forward svc/ibmfhir 9443:9443 -n fybrik-system  
 - curl -k -u 'fhiruser:change-password' 'https://127.0.0.1:9443/fhir-server/api/v4/Patient'