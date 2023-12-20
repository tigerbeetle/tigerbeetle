variable "instance_id" {
  type = string
}

variable "storage_type" {
  type = string
  default = "nvme" # or ram, or ebs
}

variable "cluster_id" {
  type = string
  default = "0"
}

variable "test_id" {
  type = string
}

variable "replica" {
  type = string
}

variable "replica_count" {
  type = string
}

variable "bind_port" {
  type = string
  default = "3001"
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
        memory_hard_limit = 60000

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
./zig/zig build install -Drelease

if ! [ -e "/tank/{{ env "NOMAD_JOB_ID" }}.tigerbeetle" ]; then
  ./tigerbeetle format --cluster=${var.cluster_id} --replica=${var.replica} --replica-count=${var.replica_count} /tank/{{ env "NOMAD_JOB_ID" }}.tigerbeetle
fi

exec ./tigerbeetle start --addresses=${var.addresses} /tank/{{ env "NOMAD_JOB_ID" }}.tigerbeetle
    EOF

        destination = "local/tigerbeetle.sh"
        perms = "755"
      }

      resources {
        cores = 1
        memory = 3192
      }
    }
  }
}
