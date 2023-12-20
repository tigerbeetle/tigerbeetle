variable "instance_id" {
  type = string
}

variable "replica_instance_ids" {
  type = string
  default = ""
}

variable "cluster_id" {
  type = string
  default = "0"
}

variable "test_id" {
  type = string
}

variable "addresses" {
  type = string
}

variable "git_url" {
  type = string
  default = "https://github.com/tigerbeetle/tigerbeetle.git"
}

variable "git_ref" {
  type = string
  default = "main"
}

job "__JOB_NAME__" {
  datacenters = ["dc1"]
  type        = "batch"

  constraint {
    attribute = attr.unique.platform.aws.instance-id
    operator  = "="
    value     = var.instance_id
  }

  reschedule {
    attempts  = 0
    unlimited = false
  }

  group "tigerbeetle-client" {
    network {
      mode = "host"
    }

    task "tigerbeetle-client" {
      driver = "docker"
      shutdown_delay = "10s"

      config {
        image = "debian:bullseye"
        entrypoint = ["/local/tigerbeetle-client.sh"]
        network_mode = "host"
      }

      template {
        data = <<EOF
#!/usr/bin/env bash
set -eux

apt-get update
apt-get -y install git curl xz-utils unzip wget awscli

# Hack - set to real region
export AWS_REGION=eu-west-1
export AWS_DEFAULT_REGION=eu-west-1

# Try do a best effort cleanup. We have TTLs and a reaper process, so there's
# no risk of leaking machines, but no sense in letting them run longer than
# they need to, either.
function finish {
  # Shut down all instances - the instance role has permission to do this
  aws ec2 terminate-instances --instance-ids ${var.replica_instance_ids} || true

  # Purge Nomad jobs, then terminate this instance. We have a shutdown delay of
  # 10s which should be plenty of time.
  if [ -e /tmp/nomad ]; then
    (/tmp/nomad job status | grep tigerbeetle-${var.test_id} | awk '{print $1}' | xargs -L1 /tmp/nomad job stop -purge) || true
  else
    echo "No Nomad yet - not purging."
  fi

  aws ec2 terminate-instances --instance-ids ${var.instance_id} || true
}
trap finish EXIT

# Fetch and extract Nomad
cd /tmp
wget https://releases.hashicorp.com/nomad/1.5.3/nomad_1.5.3_linux_amd64.zip
unzip nomad_1.5.3_linux_amd64.zip
chmod +x nomad

git config --global remote.origin.fetch '+refs/pull/*:refs/remotes/origin/pull/*'
git clone ${var.git_url}
cd tigerbeetle
git checkout ${var.git_ref}
./scripts/install_zig.sh

cmd="./zig/zig build benchmark -Drelease -- --account-count=10000 --transfer-count=10000000 --transfer-count-per-second=1000000 --addresses=${var.addresses} --statsd --print-batch-timings"
echo "TigerBeetle Benchmark Command: ${cmd}"
timeout -s KILL 3400 $cmd

# Ensure time for results to have shipped
sleep 10
    EOF

        destination = "local/tigerbeetle-client.sh"
        perms = "755"
      }

      resources {
        cores = 1
        memory = 14000
      }
    }
  }
}
