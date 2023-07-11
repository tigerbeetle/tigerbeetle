# VOPR Server Setup

*The VOPR Server is a dedicated testing machine. It continually runs VOPR simulators (performing deterministic fuzz testing) as well as another program, called the VOPR Hub (that listens out for bug reports). When a VOPR finds a bug, it creates a report and sends it to the VOPR Hub. When a report comes in the hub has an additional, dedicated VOPR to use to rerun the test. The hub then automatically creates a GitHub issue with the collected debug logs and stack trace.*

*To ensure the latest code is always being tested, the VOPRs restart after running only a few seeds. Before restarting, the scheduler is responsible for fetching the latest code and checking out the relevant commit for that VOPR. At least one VOPR will always run on main. Other VOPRs can be assigned to test pull requests with the `vopr` label. The latest pull requests are always favored if there are too many to test.*

*Follow these instructions to set up the VOPR Hub, along with several full-time VOPRs.*

## Server Setup

Install Go:
```bash
sudo add-apt-repository ppa:longsleep/golang-backports
sudo apt update
sudo apt install golang-go
```

Add two users, namely `voprrunner` and `voprhub`. This creates *separation of concerns* between the two functions of the server.

The `voprrunner` will continuously run the VOPR and send any bugs to the VOPR Hub.

This will require setting passwords for the respective users:

```bash
sudo adduser voprhub
sudo adduser voprrunner
sudo usermod -aG sudo voprhub
sudo usermod -aG sudo voprrunner
```

## Set Up the VOPR Hub Component

Become the `voprhub` user:
```bash
su - voprhub
```

Clone `tigerbeetle`:
```bash
git clone https://github.com/tigerbeetle/tigerbeetle.git
```

Install Zig:
```bash
cd ./tigerbeetle
./scripts/install_zig.sh
cd ../
```

Create a second `tigerbeetle` directory here inside the hub directory which will run the VOPR Hub. The initial `tigerbeetle` directory will be needed to replay any seeds that the hub receives.
```bash
mkdir hub
cp -r tigerbeetle hub/tigerbeetle
```

Create a `systemd` service unit file for the hub:
```bash
sudo nano /etc/systemd/system/voprhub.service
```
The file should contain the following (including an actual IP address and developer token with access to public repositories):
```
[Unit]

Description=Continuously runs the VOPR Hub.

[Service]

User=voprhub
WorkingDirectory=/home/voprhub/hub/tigerbeetle/src/vopr_hub
Environment="REPOSITORY_URL=https://api.github.com/repos/tigerbeetle/tigerbeetle"
Environment="TIGERBEETLE_DIRECTORY=/home/voprhub/tigerbeetle"
Environment="VOPR_HUB_ADDRESS=<address>"
Environment="ISSUE_DIRECTORY=/home/voprhub"
Environment="DEVELOPER_TOKEN=******"
ExecStart=go run main.go
Restart=on-success

[Install]

WantedBy=multi-user.target
```

Start the VOPR Hub service:
```bash
systemctl start voprhub.service
# Check that we have liftoff.
systemctl status voprhub.service
# View logs e.g.
journalctl -f -n 100 -u voprhub.service
```

Go back to the `root` user:
```bash
exit
```

## Set Up the VOPR Component

Become the `voprrunner` user:
```bash
su - voprrunner
```

Create a script that will be used by the service to fetch the latest code and run the VOPR:
```bash
sudo nano vopr_runner.sh
```

The file should contain the following (including an actual IP address and developer token with access to public repositories):
```
#!/usr/bin/env bash
set -e

# Checkout the correct branch
export TIGERBEETLE_DIRECTORY="/home/voprrunner/tigerbeetle"
export REPOSITORY_URL="https://api.github.com/repos/tigerbeetle/tigerbeetle"
export DEVELOPER_TOKEN="******"
export NUM_VOPRS="4"
export CURRENT_VOPR=$1
go run ./src/vopr_hub/scheduler/main.go

# Run the VOPR a few times before we go back to the top:
zig/zig run ./src/vopr.zig -- --send="<address>" --simulations=100
```

Ensure the script is executable:
```bash
sudo chmod +x vopr_runner.sh
```

Create four `tigerbeetle` directories here.

Note that the number of directories corresponds to the number of service instances that will run.

Ideally, this number should be increased/decreased to be two less than the number of CPU cores available. The simulators burn CPU, and so this allocation leaves a core available for the rest of the system, plus a core for the hub itself.
```bash
git clone https://github.com/tigerbeetle/tigerbeetle.git
# Install Zig:
cd ./tigerbeetle
./scripts/install_zig.sh
cd ../
# Copy this directory to get four tigerbeetle directories.
cp -r tigerbeetle tigerbeetle0 # Repeat with incrementing values for the other instances. E.g. 1, 2,3.
# Now we can remove the original directory.
sudo rm -r tigerbeetle
```

Create a `systemd` service unit file.

Naming the file `vopr@.service` means that it acts as a template that can reuse the same file to run different services that each target their own directories.
```bash
sudo nano /etc/systemd/system/vopr@.service
```

The file should contain the following:
```
[Unit]
Description=Continuously runs the VOPR.
PartOf=vopr.target

[Service]

User=voprrunner
WorkingDirectory=/home/voprrunner/tigerbeetle%i
ExecStart=/home/voprrunner/vopr_runner.sh %i
Restart=on-success

[Install]

WantedBy=multi-user.target
```

Create a target file to manage all instances, called `vopr.target`.

Dependencies must be listed under `Wants` instead of `Requires` because requiring the services will cause them all to restart whenever one terminates.
```bash
sudo nano /etc/systemd/system/vopr.target
```

The file should contain the following:
```
[Unit]
Description=Runs all VOPR services.
Wants=vopr@0.service vopr@1.service vopr@2.service vopr@3.service

[Install]
WantedBy=multi-user.target
```

Start all services:
```bash
systemctl start vopr.target
# Check it's VOPR'izing:
systemctl status vopr.target
# Check that the individual VOPR'raptors have started up:
systemctl status vopr@0.service
# View logs e.g.
journalctl -f -n 100 -u vopr@0.service
```
