gpsd to gpslogger endpoint gateway
        
Configurable gateway to send gpsd TPV (Time Position Velocity) data to a GPSLogger-compatible endpoint

Example usage:
* gps receiver (standalone or embedded in a device like a Peplink router) connected to gpsd on a SBC
* a mapping/tracking service (like Reitti) which supports gpslogger but not gpsd running on the SBC
* this gateway running on the SBC to automatically get live GPS data from gpsd into the mapping/tracking app

Related programs:
* gpsd - https://gpsd.gitlab.io/gpsd/
    * gpsd is a service daemon that monitors one or more GPSes or AIS receivers attached to a host computer through serial or USB ports, making all data on the location/course/velocity of the sensors available to be queried on TCP port 2947 of the host computer.
* Reitti - https://github.com/dedicatedcode/reitti or https://www.dedicatedcode.com/projects/reitti/latest/
    * Reitti is a comprehensive personal location tracking and analysis application that helps you understand your movement patterns and significant places. The name "Reitti" comes from Finnish, meaning "route" or "path".

Possible improvements:
* Optional "Significant change" support, only updating if position or velocity changes significantly (or perhaps a scalable update rate)
* Make the gpslogger payload configurable, perhaps borrowing the same customizable format gpslogger uses
* Send batches of downsampled data (example: one batch of 60 TPV messages, each point 15 seconds apart) for greater mapping/tracking service efficiency
* Support different upload types that gpslogger supports, not just the Custom URL endpoint
* Limit queue sizes as to not use all memory if gpslogger endpoint is down for an long time
* Add support for other TPV sources, such as Starlink Precise location data via gRPC - now available only for high-tier Priority accounts (including Priority Local, Priority, and Global Priority)
* Replace this Python gateway by refactoring gpslogger Android app into seperate GUI and backend service code, make the service run under non-Android Linux, and add location source support for gpsd under Linux in addition to the Android Framework Location API (gpslogger dropped support for the Google Play Services Location API), thereby making gpslogger its own gpsd-to-gpslogger-compatible-endpoint gateway
