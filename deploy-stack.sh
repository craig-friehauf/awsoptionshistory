#! /bin/bash
# Script to deploy OptionsHistory AWS Cloudformation Stack

# See REAMME.md of information how these varaibles effect the stack
STACKNAME='OptionsHistory'
REGION='us-west-1'
S3BUCKET='craigfriehauf-sanfran'
SNSEMAIL='contact.craigfriehauf@gmail.com'
#XRAY='TRUE'
BUILDDASHBOARD='OptionsHistory-Monitoring-Test'
LOGLEVEL='ERROR'
# See README.md for more details on AWS formated cron schedules
CRONSCHEDULE='cron(0 14,17,20 ? * 2-6 *)'

_getlambdapackageinfo () {
	# helper function for managing parameters name and s3keys for lambda
	# Unset varibale and then load them based on package
	unset PACKNAME RTIME BTIME THISS3BUCKET THISS3PREFIX LAMUPDATE THISS3KEY
	if [[ -a "${1}/package.info" ]]; then
		
		read PACKNAME RTIME BTIME THISS3BUCKET THISS3PREFIX LAMUPDATE \
			< <(cat "${1}/package.info")
		
		if ! [[ -n THISS3PREFIX ]]; then
			echo "LambdaPackage -- ${1} has not been packaged yet"
			exit
		fi
		THISS3KEY=${THISS3PREFIX}/${PACKNAME}${BTIME}.zip
		return 0
	fi
}	

_push2s3 () {
	# Common function to push packaged code to s3
	# supress is argument is passed into the script
	if [[ -n "${2}" ]]; then
		return 0
	fi

	aws s3 cp "${1}/${PACKNAME}${BTIME}.zip" \
		"s3://${S3BUCKET}/${THISS3KEY}"

	if ! [[ $? -eq 0 ]]; then
		echo "Failed to push ${1}/${PACKNAME}${BTIME}.zip packaged code to s3
		Check s3 bucket shell variable and if package exists"
		exit
	fi	

}

# Set params for lambaproxyfunc and push package to s3
_getlambdapackageinfo ./lambdaproxyfunc/function
param1="lambdaproxyfuncname=${PACKNAME}"
param2="lambdaproxyfuncs3key=${THISS3KEY}"
_push2s3 './lambdaproxyfunc' "${1}"

# Set params for collectdatafunc and push package to s3
_getlambdapackageinfo ./collectdatafunc/function
param3="collectdatafuncname=${PACKNAME}"
param4="collectdatafuncs3key=${THISS3KEY}"
_push2s3 './collectdatafunc' "${1}"

# Set params for collectdatafunclayer
_getlambdapackageinfo ./collectdatafunc/layer
param5="collectdatalayers3key=${THISS3KEY}"
_push2s3 './collectdatafunc' "${1}"

# get params for logprocessingfunc
_getlambdapackageinfo ./logprocessingfunc/function
param6="logprocessingfuncname=${PACKNAME}"
param7="logprocessingfuncs3key=${THISS3KEY}"
_push2s3 './logprocessingfunc' "${1}"

# get params for streamprocessingfunc
_getlambdapackageinfo ./streamprocessingfunc/function 
param8="streamprocessingfuncname=${PACKNAME}"
param9="streamprocessingfuncs3key=${THISS3KEY}"
_push2s3 './streamprocessingfunc' "${1}"

# get params for xraylayer
_getlambdapackageinfo ./xraylayer/layer
param10="pythonxraysdklayers3key=${THISS3KEY}"
_push2s3 './xraylayer' "${1}"

aws cloudformation deploy --stack-name "${STACKNAME}" \
	--template-file ./serverlessstack.yaml \
	--capabilities CAPABILITY_IAM \
	--parameter-overrides \
	"${param1}" "${param2}" "${param3}" "${param4}" "${param5}" \
	"${param6}" "${param7}" "${param8}" "${param9}" "${param10}" \
	"logprocessingsnssubscriber=${SNSEMAIL}" \
	"s3bucket=${S3BUCKET}" "activatexray=${XRAY}" \
	"cronschedule=${CRONSCHEDULE}" \
	"builddashboard=${BUILDDASHBOARD}" \
	"loglevellambdafunctions=${LOGLEVEL}" \
	--region "${REGION}"

if [[  $? -eq 255  ]]; then
	# Stack failed to be deployed dumping event log to local directory
	echo "Stack Failed to Deploy dumping stack-events to ./stackeventlog.json"
	aws cloudformation describe-stack-events \
		--stack-name "${STACKNAME}" > stackeventlog.json
else
	# The stack deployed sucessfully dump API Gateway APIEndpoint to
	# local file
	aws cloudformation describe-stacks \
	--stack-name "${STACKNAME}" \
	--region "${REGION}" \
	--query "Stacks[0].Outputs[0].OutputValue" \
	--output text > APIEndpoint

	cp APIEndpoint ./clientexample 
fi
