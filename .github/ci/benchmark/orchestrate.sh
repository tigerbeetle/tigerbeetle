#!/usr/bin/env bash
set -eu -o pipefail
REPLICA_COUNT="${1}"
AWS_INSTANCE_TYPE="${2}"

AMI_ID="ami-05227d1de84630a07"
SECURITY_GROUP_IDS="sg-0cfff65f4c5b93588 sg-042df46f5cea3faf8 sg-0e9bb6ac7fa2d665b"
SUBNET_ID="subnet-05a08d203fecbf048"

TEST_ID=$(uuidgen)

REPLICA_INSTANCE_IDS=()
REPLICA_PRIVATE_IPS=()

# Pull down AWS creds from Nomad
AWS_ACCESS_KEY_ID=$(nomad var get -item=AWS_ACCESS_KEY_ID aws)
export AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY=$(nomad var get -item=AWS_SECRET_ACCESS_KEY aws)
export AWS_SECRET_ACCESS_KEY

export AWS_REGION=eu-west-1
export AWS_DEFAULT_REGION=eu-west-1

# Spin up EC2 instances
echo "Spinning up replica EC2 instances - ${REPLICA_COUNT}..." 1>&2
epoch_plus_hour=$(($(date +%s) + 3600))

# shellcheck disable=SC2086 # we hardcode SECURITY_GROUP_IDS and need it to be multiple parameters
output=$(aws ec2 run-instances \
	--image-id "${AMI_ID}" \
	--count "${REPLICA_COUNT}" \
	--instance-type "${AWS_INSTANCE_TYPE}" \
	--security-group-ids ${SECURITY_GROUP_IDS} \
	--subnet-id "${SUBNET_ID}" \
	--associate-public-ip-address \
	--iam-instance-profile Name="worker-profile" \
	--tag-specifications "ResourceType=instance,Tags=[{Key=test_id,Value=${TEST_ID}},{Key=ttl,Value=${epoch_plus_hour}}]" \
)

for i in $(seq 1 "${REPLICA_COUNT}"); do
	# 0-index our replicas
	i=$((i - 1))

	REPLICA_INSTANCE_IDS+=("$(echo "${output}" | jq -r .Instances[${i}].InstanceId)")
	REPLICA_PRIVATE_IPS+=("$(echo "${output}" | jq -r .Instances[${i}].PrivateIpAddress)":3001)
done

# The below are set for both client and replicas.
# We fix our git URL, so we'll only ever run this on our own branches and not random PRs
export NOMAD_VAR_git_url="https://github.com/tigerbeetle/tigerbeetle.git"
export NOMAD_VAR_git_ref="${3}"

# Build the address string
function join_by { local IFS="$1"; shift; echo "$*"; }
addresses=$(join_by , "${REPLICA_PRIVATE_IPS[@]}")

# Template and spawn each Tigerbeetle replica's Nomad job.
for replica in $(seq 1 "${REPLICA_COUNT}"); do
	# 0-index our replicas
	replica=$((replica - 1))
	JOB_NAME="tigerbeetle-${TEST_ID}-${replica}"

	export NOMAD_VAR_instance_id=${REPLICA_INSTANCE_IDS[$replica]}
	export NOMAD_VAR_test_id="${TEST_ID}"
	export NOMAD_VAR_replica="${replica}"
	export NOMAD_VAR_replica_count="${REPLICA_COUNT}"
	export NOMAD_VAR_addresses="${addresses}"

	sed "s/__JOB_NAME__/${JOB_NAME}/g" < tigerbeetle.hcl | nomad job run -detach - 1>&2
done

# Spin up EC2 instance for test client
echo "Spinning up client EC2 instance..." 1>&2
# shellcheck disable=SC2086 # we hardcode SECURITY_GROUP_IDS and need it to be multiple parameters
output=$(aws ec2 run-instances \
	--image-id "${AMI_ID}" \
	--count 1 \
	--instance-type "t3.xlarge" \
	--security-group-ids ${SECURITY_GROUP_IDS} \
	--subnet-id "${SUBNET_ID}" \
	--associate-public-ip-address \
	--iam-instance-profile Name="worker-profile" \
	--tag-specifications "ResourceType=instance,Tags=[{Key=test_id,Value=${TEST_ID}},{Key=ttl,Value=${epoch_plus_hour}}]" \
)

CLIENT_INSTANCE_ID=$(echo "${output}" | jq -r .Instances[0].InstanceId)

# Spin up test client Nomad Job
JOB_NAME="tigerbeetle-${TEST_ID}-client"

export NOMAD_VAR_instance_id=${CLIENT_INSTANCE_ID}
export NOMAD_VAR_test_id="${TEST_ID}"
export NOMAD_VAR_addresses="${addresses}"
export NOMAD_VAR_replica_instance_ids="${REPLICA_INSTANCE_IDS[*]}"

sed "s/__JOB_NAME__/${JOB_NAME}/g" < client.hcl | nomad job run -detach - 1>&2

epoch_ms=$(date +%s%N | cut -b1-13)
echo "https://grafana.arewe1mtpsyet.com/d/5_Y-UcE4z/test-dashboard?orgId=1&from=${epoch_ms}&to=now-30s&var-test_id=${TEST_ID}"
