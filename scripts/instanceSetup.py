# -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
# Install plugin
# -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
INSTALL_PLUGIN = """
#!/bin/bash
set -e
cd /tmp

BUCKET={{ params.bucket }}
DATAFUSION_INSTANCE_NAME={{ params.datafusionInstanceName }}
PLUGIN_NAME={{ params.pluginName }}
PLUGIN_JAR={{ params.pluginJar }}
PLUGIN_VERSION={{ params.pluginVersion }}
PLUGIN_PROPERTIES={{ params.pluginProperties }}
VERSION_EXTENDS={{ params.pluginExtends }}

PLUGIN_EXTENDS="system:cdap-data-pipeline[$VERSION_EXTENDS,7.0.0-SNAPSHOT)/system:cdap-data-streams[$VERSION_EXTENDS,7.0.0-SNAPSHOT)"

gsutil cp $BUCKET/$PLUGIN_JAR .
gsutil cp $BUCKET/$PLUGIN_PROPERTIES .

export AUTH_TOKEN=$(gcloud auth print-access-token)
export CDAP_ENDPOINT=$(gcloud beta data-fusion instances describe \
        --location=europe-west1 \
        --format="value(apiEndpoint)" $DATAFUSION_INSTANCE_NAME)

curl -X POST  -H "Authorization: Bearer "$AUTH_TOKEN \
    $CDAP_ENDPOINT/v3/namespaces/default/artifacts/$PLUGIN_NAME \
    -H "Artifact-Version: $PLUGIN_VERSION" \
    -H "Artifact-Extends: $PLUGIN_EXTENDS" \
    --data-binary @$PLUGIN_JAR

curl -X PUT -H "Authorization: Bearer "$AUTH_TOKEN \
    $CDAP_ENDPOINT/v3/namespaces/default/artifacts/$PLUGIN_NAME/versions/$PLUGIN_VERSION/properties \
    --data-binary @$PLUGIN_PROPERTIES
   """

# -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
# create datafusion instance
# -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
CREATE_DF = """
#!/bin/bash

PROJECT_ID={{ params.project_id }}
LOCATION={{ params.location }}
INSTANCE_NAME={{ params.datafusionInstanceName }}
INSTANCE_TYPE={{ params.datafusion_instance_type}}
VPC_NAME={{ params.vpc_name }}
IP_RANGE={{ params.ip_range }}
COMPUTE_ACCOUNT={{ params.compute_account }}

OPERATION=$(curl -H "Authorization: Bearer $(gcloud auth print-access-token)" -H "Content-Type: application/json" https://datafusion.googleapis.com/v1/projects/$PROJECT_ID/locations/$LOCATION/instances?instance_id=$INSTANCE_NAME -X POST -d '{"description": "Datafusion instance.", "type": "'$INSTANCE_TYPE'", "privateInstance": true, "networkConfig": {"network": "'$VPC_NAME'", "ipAllocation": "'$IP_RANGE'"}}')
echo 'Response='$OPERATION


ERROR=$(echo $OPERATION | grep -Po '"error": *\K{[^}]*"'| sed -e 's/^"//' -e 's/"$//')


echo $ERROR
if [[ $(echo $OPERATION | grep -Po '"code": *\K[^,]*') == '409' ]];
    then echo 'instance already existed!'
    exit 0
elif [[ "$ERROR" != "" ]];
    then echo 'ERROR='$(echo $ERROR | grep -Po '"message": *\K"[^"]*"' | grep -o '"[^"]*"$'| sed -e 's/^"//' -e 's/"$//');
    exit 1
else
    echo 'Creation initiated';
fi


OPNAME=$(echo $OPERATION | grep -Po '"name": *\K"[^"]*"'| grep -o '"[^"]*"$'| sed -e 's/^"//' -e 's/"$//')
echo 'Operation='$OPNAME
STATE=$(curl -H "Authorization: Bearer $(gcloud auth print-access-token)" -X GET https://datafusion.googleapis.com/v1beta1/$OPNAME | grep -Po '"state": *\K"[^"]*"'| grep -o '"[^"]*"$'| sed -e 's/^"//' -e 's/"$//')
echo $STATE
while [[ "$STATE" != "ACTIVE" ]]
do
    echo 'sleeping for 60 seconds...'
    sleep 60
    echo 'polling operations api to check datafusion instance activity...'
    STATE=$(curl -H "Authorization: Bearer $(gcloud auth print-access-token)" -X GET https://datafusion.googleapis.com/v1beta1/$OPNAME | grep -Po '"state": *\K"[^"]*"'| grep -o '"[^"]*"$'| sed -e 's/^"//' -e 's/"$//')
    echo $STATE
done

echo 'data fusion instance created'
exit 0
"""
