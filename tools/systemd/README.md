# systemd Service Unit

This is an example of a systemd service that runs TigerBeetle.
You will likely have to adjust it if you're deploying TigerBeetle.

## Adjusting

You can adjust multiple aspects of this systemd service.
Each specific adjustment is listed below with instructions.

It is not recommended to adjust some values directly in the service file.
When this is the case, the instructions will ask you to instead use systemd's drop-in file support.
Here's how to do that:

1. Install the service unit in systemd (usually by adding it to `/etc/systemd/system`).
2. Create a drop-in file to override the environment variables.
   Run `systemctl edit tigerbeetle.service`.
   This will bring you to an editor with instructions.
3. Add your overrides.
   Example:
   ```
   [Service]
   Environment=TIGERBEETLE_CACHE_GRID_SIZE=4GB
   Environment=TIGERBEETLE_ADDRESSES=0.0.0.0:3001
   ```

### Pre-start script

This service executes the `tigerbeetle-pre-start` script before starting TigerBeetle.
This script is responsible for ensuring that a replica data file exists.
It will create a data file if it doesn't exist.

The script assumes that `/bin/sh` exists and points to a POSIX-compliant shell, and the `test` utility is either built-in or in the script's search path.
If this is not the case, adjust the script's shebang.

The service assumes that this script is installed in `/usr/local/bin`.
If this is not the case, adjust the `ExecStartPre` line in `tigerbeetle.service`.

### TigerBeetle executable

The `tigerbeetle` executable is assumed to be installed in `/usr/local/bin`.
If this is not the case, adjust both `tigerbeetle.service` and `tigerbeetle-pre-start` to use the correct location.

### Environment variables

This service uses environment variables to provide default values for a simple single-node cluster.
To configure a different cluster structure, or a cluster with different values, adjust the values in the environment variables.
It is **not recommended** to change these default values directly in the service file, because it may be important to revert to the default behavior later.
Instead, use systemd's drop-in file support.

### State directory and replica data file path

This service configures a state directory, which means that systemd will make sure the directory is created before the service starts, and the directory will have the correct permissions.
This is especially important because the service uses systemd's dynamic user capabilities.
systemd forces the state directory to be in `/var/lib`, which means that this service will have its replica data file at `/var/lib/tigerbeetle/`.
It is **not recommended** to adjust the state directory directly in the service file, because it may be important to revert to the default behavior later.
Instead, use systemd's drop-in file support.
If you do so, remember to also adjust the `TIGERBEETLE_DATA_FILE` environment variable, because it also hardcodes the `tigerbeetle` state directory value.

Due to systemd's dynamic user capabilities, the replica data file path will not be owned by any existing user of the system.

### Hardening configurations

Some hardening configurations are enabled for added security when running the service.
It is **not recommended** to change these, since they have additional implications on all other configurations and values defined in this service file.
If you wish to change those, you are expected to understand those implications and make any other adjustments accordingly.
