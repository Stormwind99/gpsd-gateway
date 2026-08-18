# gpsd gateway
        
Configurable gateway to send gpsd TPV (Time Position Velocity) data to different endpoints

Supported endpoints:
* Custom URL sender (compatible with GPSLogger Custom URL sender)
* HTTP File Upload which uploads a GPX file (compatible with GPSLogger HTTP File Upload sender)

Features:
* Can take a sample every X seconds from the samples received from gpsd, to reduce number of points and thus load and data sizes over time
* Can batch samples for Y seconds, then send the samples in one batch as GPX data

## Project Links

* Source: https://github.com/Stormwind99/gpsd-gateway/
* Container images: https://ghcr.io/stormwind99/gpsd-gateway
   * Latest: ghcr.io/stormwind99/gpsd-gateway:latest

## Examples

### Main scenario

* A GPS receiver (standalone or embedded in a device like a Peplink router) connected to gpsd on a SBC
* A mapping/tracking service (like Reitti) which supports a GPSLogger endpoint (but not gpsd) running on the SBC
* gpsd-gateway running on the SBC to automatically get live GPS data from gpsd into the mapping/tracking app that supports GPSLogger endpoings

### Example usage

* Print the help for all command line options (and config file options)
   * ```python3 ./gpsd-gateway.py  --help```
* Connect gpsd running on gpsdhost.internal port 2947 to Reitti's gpslogger endpoint (emulating GPSLogger's Custom URL sender) with security token API_TOKEN using default options (send one point each 15 seconds)
   * ```python3 gpsd-gateway.py -s gpsd://gpsdhost.internal:2947 -u http://reittihost.internal:8080/api/v1/ingest/gpslogger -t API_TOKEN```
* Batching: Connect gpsd running on gpsdhost.internal port 2947 to Reitti's gpx import endpoint (emulating GPSLogger's HTTP File Upload sender) with security token API_TOKEN sampling one point each 5 seconds and sending one batch of points every 60 seconds
   * ```python3 gpsd-gateway.py -s gpsd://gpsdhost.internal:2947 -u http://reittihost.internal:8080/api/v1/gpx/import -t API_TOKEN -i 5 -b 60```

## Other software

### Related software

* gpsd - https://gpsd.gitlab.io/gpsd/
    * gpsd is a service daemon that monitors one or more GPSes or AIS receivers attached to a host computer through serial or USB ports, making all data on the location/course/velocity of the sensors available to be queried on TCP port 2947 of the host computer.
* Reitti - https://github.com/dedicatedcode/reitti or https://www.dedicatedcode.com/projects/reitti/latest/
    * Reitti is a comprehensive personal location tracking and analysis application that helps you understand your movement patterns and significant places. The name "Reitti" comes from Finnish, meaning "route" or "path".
* GPSLogger - https://github.com/mendhak/gpslogger
    * GPSLogger is an Android app that logs GPS information to various formats (GPX, KML, CSV, NMEA, Custom URL) and has options for uploading (SFTP, OpenStreetMap, Google Drive, Dropbox, Email). 

### Similar software

* owntracks-cli-publisher - https://github.com/owntracks/ocli
    * OwnTracks command line interface publisher, a.k.a. owntracks-cli-publisher, is a small utility which connects to gpsd and publishes position information in OwnTracks JSON to an MQTT broker in order for compatible software to process location data.

## Possible improvements

* Support more upload types that GPSLogger supports, not just the Custom URL endpoint and HTTP File Upload endpoint
* Limit queue sizes as to not use all memory if GPSLogger endpoint is down for a long time
* Optional "Significant change" support, only updating if position or velocity changes significantly (or perhaps a scalable update rate)
* Add support for other TPV sources, such as Starlink Precise location data via gRPC - now available only for high-tier Priority accounts (including Priority Local, Priority, and Global Priority)
* Replace this Python gateway by refactoring GPSLogger Android app into seperate GUI and backend service code, make the service run under non-Android Linux, and add location source support for gpsd under Linux in addition to the Android Framework Location API (GPSLogger dropped support for the Google Play Services Location API), thereby making GPSLogger its own gpsd gateway
