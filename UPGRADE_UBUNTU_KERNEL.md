# Upgrade Ubuntu to the 5.7.15 kernel

We use 5.7.15 for benchmarking `io_uring` because there is a [network performance regression as from 5.7.16](https://github.com/axboe/liburing/issues/215) that is being patched.

These are the direct links to the 5.7.15 amd64 generic kernel files (note the "_all.deb" or "generic" keywords):

```
# Download:
wget https://kernel.ubuntu.com/~kernel-ppa/mainline/v5.7.15/amd64/linux-headers-5.7.15-050715_5.7.15-050715.202008111432_all.deb
wget https://kernel.ubuntu.com/~kernel-ppa/mainline/v5.7.15/amd64/linux-headers-5.7.15-050715-generic_5.7.15-050715.202008111432_amd64.deb
wget https://kernel.ubuntu.com/~kernel-ppa/mainline/v5.7.15/amd64/linux-image-unsigned-5.7.15-050715-generic_5.7.15-050715.202008111432_amd64.deb
wget https://kernel.ubuntu.com/~kernel-ppa/mainline/v5.7.15/amd64/linux-modules-5.7.15-050715-generic_5.7.15-050715.202008111432_amd64.deb

# Install and reboot:
sudo dpkg -i *.deb
sudo reboot

# Check kernel version:
uname -sr
```

For newer than 5.7.15, you can also find the full list of [mainline releases here](https://kernel.ubuntu.com/~kernel-ppa/mainline/?C=N;O=D).
