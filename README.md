# gpsd gateway
        
Configurable gateway to send gpsd TPV (Time Position Velocity) data to different endpoints (a GPSLogger-compatible endpoint or a GPX uploader endpoint so far)

## Project Links

* Source: https://github.com/Stormwind99/gpsd-to-gpslogger-endpoint/
* Container images: https://ghcr.io/stormwind99/gpsd-to-gpslogger-endpoint
   * Latest: ghcr.io/stormwind99/gpsd-to-gpslogger-endpoint:latest

## Example usage:

* GPS receiver (standalone or embedded in a device like a Peplink router) connected to gpsd on a SBC
* A mapping/tracking service (like Reitti) which supports gpslogger but not gpsd running on the SBC
* This gateway running on the SBC to automatically get live GPS data from gpsd into the mapping/tracking app that supports gpslogger endpoings

## Related programs:

* gpsd - https://gpsd.gitlab.io/gpsd/
    * gpsd is a service daemon that monitors one or more GPSes or AIS receivers attached to a host computer through serial or USB ports, making all data on the location/course/velocity of the sensors available to be queried on TCP port 2947 of the host computer.
* Reitti - https://github.com/dedicatedcode/reitti or https://www.dedicatedcode.com/projects/reitti/latest/
    * Reitti is a comprehensive personal location tracking and analysis application that helps you understand your movement patterns and significant places. The name "Reitti" comes from Finnish, meaning "route" or "path".

## Possible improvements:

* Make the gpslogger payload configurable, perhaps borrowing the same customizable format gpslogger uses
* Support different upload types that gpslogger supports, not just the Custom URL endpoint and HTTP File Uploader endpoint
* Limit queue sizes as to not use all memory if gpslogger endpoint is down for a long time
* Optional "Significant change" support, only updating if position or velocity changes significantly (or perhaps a scalable update rate)
* Add support for other TPV sources, such as Starlink Precise location data via gRPC - now available only for high-tier Priority accounts (including Priority Local, Priority, and Global Priority)
* Replace this Python gateway by refactoring gpslogger Android app into seperate GUI and backend service code, make the service run under non-Android Linux, and add location source support for gpsd under Linux in addition to the Android Framework Location API (gpslogger dropped support for the Google Play Services Location API), thereby making gpslogger its own gpsd-to-gpslogger-compatible-endpoint gateway
