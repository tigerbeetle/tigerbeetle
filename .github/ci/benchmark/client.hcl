variable "replica_instance_ids" {
  type = string
  default = ""
}

variable "cluster_id" {
  type = string
  default = "0"
}

variable "addresses" {
  type = string
  default = "127.0.0.1:3001"
}

variable "git_url" {
  type = string
  default = "https://github.com/tigerbeetle/tigerbeetle.git"
}

variable "git_ref" {
  type = string
  default = "cb22/count-von-count"
}

job "tigerbeetle-client" {
  datacenters = ["dc1"]
  type        = "batch"

  constraint {
    attribute = attr.unique.hostname
    operator  = "="
    value     = "Debian-1201-bookworm-amd64-base"
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

git config --global remote.origin.fetch '+refs/pull/*:refs/remotes/origin/pull/*'
git clone ${var.git_url}
cd tigerbeetle
git checkout ${var.git_ref}
./scripts/install_zig.sh

cmd="./zig/zig build benchmark -Drelease -- --account-count=10000 --transfer-count=100000000000 --transfer-count-per-second=1000000 --addresses=${var.addresses} --statsd --print-batch-timings"
echo "TigerBeetle Benchmark Command: ${cmd}"
$cmd

# Ensure time for results to have shipped
sleep 10
    EOF

        destination = "local/tigerbeetle-client.sh"
        perms = "755"
      }

      resources {
        cores = 1
        memory = 128000
      }
    }
  }
}
