variable "storage_type" {
  type = string
  default = "nvme" # or ram, or ebs
}

variable "cluster_id" {
  type = string
  default = "0"
}

variable "replica" {
  type = string
  default = "0"
}

variable "replica_count" {
  type = string
  default = "1"
}

variable "bind_port" {
  type = string
  default = "3001"
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
  default = "main"
}

job "tigerbeetle-replica" {
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

  group "tigerbeetle" {
    network {
      mode = "host"

      port "tigerbeetle" {
        static = "${var.bind_port}"
      }
    }

    task "tigerbeetle" {
      driver = "docker"

      config {
        image = "debian:bullseye"
        entrypoint = ["/local/tigerbeetle.sh"]
        network_mode = "host"

        mounts = [
          {
            type   = "bind",
            source = "/tank/${var.storage_type}",
            target = "/tank"
          }
        ]
      }

      template {
        data = <<EOF
#!/bin/sh
set -eux

apt-get update
apt-get -y install git curl xz-utils unzip wget
cd /tmp

git config --global remote.origin.fetch '+refs/pull/*:refs/remotes/origin/pull/*'
git clone ${var.git_url}
cd tigerbeetle
git checkout ${var.git_ref}
./scripts/install_zig.sh
./zig/zig build install -Drelease -Dconfig=production

rm -f "/tank/{{ env "NOMAD_JOB_ID" }}.tigerbeetle"
./tigerbeetle format --cluster=${var.cluster_id} --replica=${var.replica} --replica-count=${var.replica_count} /tank/{{ env "NOMAD_JOB_ID" }}.tigerbeetle

exec ./tigerbeetle start --addresses=${var.addresses} --cache-grid=256GB --memory-lsm-manifest=4293918720 /tank/{{ env "NOMAD_JOB_ID" }}.tigerbeetle
    EOF

        destination = "local/tigerbeetle.sh"
        perms = "755"
      }

      resources {
        cores = 1
        memory = 512000
      }
    }
  }
}
