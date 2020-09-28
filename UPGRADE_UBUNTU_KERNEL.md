# Upgrade Ubuntu to the 5.8 kernel

These are the direct links to the 5.8.10 amd64 generic kernel files (note the "_all.deb" or "generic" keywords):

```
# Download:
wget https://kernel.ubuntu.com/~kernel-ppa/mainline/v5.8.10/amd64/linux-headers-5.8.10-050810_5.8.10-050810.202009171232_all.deb
wget https://kernel.ubuntu.com/~kernel-ppa/mainline/v5.8.10/amd64/linux-headers-5.8.10-050810-generic_5.8.10-050810.202009171232_amd64.deb
wget https://kernel.ubuntu.com/~kernel-ppa/mainline/v5.8.10/amd64/linux-image-unsigned-5.8.10-050810-generic_5.8.10-050810.202009171232_amd64.deb
wget https://kernel.ubuntu.com/~kernel-ppa/mainline/v5.8.10/amd64/linux-modules-5.8.10-050810-generic_5.8.10-050810.202009171232_amd64.deb

# Install and reboot:
sudo dpkg -i *.deb
sudo reboot

# Check kernel version:
uname -sr
```

For newer than 5.8, you can also find the full list of [mainline releases here](https://kernel.ubuntu.com/~kernel-ppa/mainline/?C=N;O=D).
