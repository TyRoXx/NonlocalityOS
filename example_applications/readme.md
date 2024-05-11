# Deploy

* successfully run `..\build.bat` (in the parent directory)
  * this is currently necessary to compile `management_service` for the target system
* successfully run `.\build.bat` in this directory
* install Raspberry Pi OS 64-bit and enable SSH with password
* copy `.env.template` as `.env`
* edit `.env` with values for your Raspberry Pi!
* run `.\example_cluster.bat deploy`
* enjoy
