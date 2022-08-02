#!/bin/bash
set -eEuo pipefail

# Assert that we are only upgrading the kernel for Ubuntu, and not another distribution:
DISTRIBUTION=$(lsb_release -i | awk '{print $3}')
if [ "$DISTRIBUTION" != "Ubuntu" ]; then
    echo "This script must be run on Ubuntu."
    exit 1
fi

# No need to downgrade the kernel if we are on a newer version.
CURRENT_KERNEL_VERSION=$(uname -r | grep -o "[0-9\.]*" | head -1)
if [ "$(printf '%s\n' "5.7.15" "$CURRENT_KERNEL_VERSION" | sort -V | head -n1)" = "5.7.15" ]; then
    echo "Current kernel ${CURRENT_KERNEL_VERSION} supports io_uring. No need to upgrade."
    exit 0
else
    echo "Upgrading kernel from ${CURRENT_KERNEL_VERSION} to 5.7.15."
fi

# Use a temporary download directory that we can cleanup afterwards:
DIRECTORY_PREFIX="upgrade_ubuntu_kernel"
rm -rf $DIRECTORY_PREFIX
mkdir $DIRECTORY_PREFIX

# Download the 5.7.15 amd64 generic kernel files (note the "_all.deb" or "generic" keywords):
echo "Downloading the 5.7.15 amd64 generic kernel files..."
wget --quiet --show-progress --directory-prefix=$DIRECTORY_PREFIX https://kernel.ubuntu.com/~kernel-ppa/mainline/v5.7.15/amd64/linux-headers-5.7.15-050715_5.7.15-050715.202008111432_all.deb
wget --quiet --show-progress --directory-prefix=$DIRECTORY_PREFIX https://kernel.ubuntu.com/~kernel-ppa/mainline/v5.7.15/amd64/linux-headers-5.7.15-050715-generic_5.7.15-050715.202008111432_amd64.deb
wget --quiet --show-progress --directory-prefix=$DIRECTORY_PREFIX https://kernel.ubuntu.com/~kernel-ppa/mainline/v5.7.15/amd64/linux-image-unsigned-5.7.15-050715-generic_5.7.15-050715.202008111432_amd64.deb
wget --quiet --show-progress --directory-prefix=$DIRECTORY_PREFIX https://kernel.ubuntu.com/~kernel-ppa/mainline/v5.7.15/amd64/linux-modules-5.7.15-050715-generic_5.7.15-050715.202008111432_amd64.deb

# Install and then remove the downloaded files:
echo "Installing (requires root)..."
sudo dpkg -i $DIRECTORY_PREFIX/*.deb
rm -rf $DIRECTORY_PREFIX
echo "Installed the 5.7.15 amd64 generic kernel files."

# Reboot the system if the user wants to:
read -p "Press Y to reboot your system (or any other key to reboot later)... " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]
then
    echo "Rebooting in 3 seconds..."
    sleep 3
    sudo reboot
else
    echo "You must reboot your system for these changes to take effect."
fi
