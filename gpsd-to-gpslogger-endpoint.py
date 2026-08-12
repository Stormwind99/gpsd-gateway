#! /usr/bin/env python3
"""
gpsd to gpslogger endpoint gateway
        
Configurable gateway to send gpsd TPV (Time Position Velocity) data to a GPSLogger-compatible endpoint

Threading model:
* main thread starts all other threads up, handles shutdown signal processing and on demand tells threads to stop, waits for threads to complete, then returns
* reader thread connects to gpsd, reads all sentences from gpsd as fast as it sends them (one per second, otherwise it will fall behind), and enqueues all unique TPV (time position velocity) messages onto the sampling queue as a gpslogger payload
* sampler thread reads the latest payload from the sampling queue at a configured interval to reduce the data rate (defaults to once per 15 seconds, so about 1 out of 15 updates) while emptying the sampling queue, and puts it into the send queue
* writer threads (default 1) wait for payloads to the send in the sending queue, and sends any payload to the gpslogger endpoint immediately (and retries any failures)
"""

import configargparse
import gps
import requests
import time
import sys
import signal
import time
import logging
import queue
import threading
from datetime import datetime

#########################################################################
# Signal processing
#########################################################################

# Interrupt key exception
class GracefulExit(BaseException):
    pass

def handle_sigterm(signum, frame) -> GracefulExit:
    """Handle SIGTERM: raise a GracefulExit exception via a function (for a signal handler)"""
    raise GracefulExit(f"Received signal {signum}")

def handle_sighup(signum, frame) -> None:
    """Handle SIGHUP: do nothing"""
    logging.info("SIGHUP received - nothing to do")

#########################################################################
# GpsdGateway
#########################################################################

class GpsdGateway:
    """
    Gateway to read GPS data from gpsd and forward it to gpslogger style endpoint
    """

    def __init__(self):
        self._lastTimestamp: int = 0
        """int: Unix timestamp of last data from gpsd session that was enqueued for sampling and potential sending"""
        self._samplingQueue: queue.Queue = queue.Queue()
        """The queue that all fully-valid-for-sending gpsd readings are enqueued to for sampling (only send 1 every X seconds)"""
        self._sendQueue: queue.Queue = queue.Queue()
        """The queue containing gpslogger payloads to send"""

    #########################################################################
    # Data processing
    #########################################################################

    def processData(session) -> dict:
        """
        Given a gpsd session object, process it and return an equivalent gpslogger-endpoint-compatible payload
        """
        logString = 'Mode: %s(%d)' % (("Invalid", "NO_FIX", "2D", "3D")[session.fix.mode], session.fix.mode)

        # battery level may be optional:
        # https://github.com/mendhak/gpslogger/blob/master/gpslogger/src/main/java/com/mendhak/gpslogger/common/SerializableLocation.java#L86
        # https://github.com/mendhak/gpslogger/blob/master/gpslogger/src/main/java/com/mendhak/gpslogger/loggers/customurl/CustomUrlLogger.java#L51

        # gpslogger text block formatting:
        # https://github.com/mendhak/gpslogger/blob/master/gpslogger/src/main/java/com/mendhak/gpslogger/senders/customurl/CustomUrlManager.java#L176
        payload = {
            '_type': 'location',
            't': 'u',
            'batt': '100',
            'bs': 'true',
        }

        # Check if latitude/longitude errors are available
        if (gps.isfinite(session.fix.epx) and gps.isfinite(session.fix.epy)):
            lat_err = session.fix.epy  # Latitude error in meters
            lon_err = session.fix.epx  # Longitude error in meters
    
            # Combine lat ang lon error to get total horizontal error margin
            horizontal_accuracy = (lat_err**2 + lon_err**2)**0.5
            logString += f" Acc: {horizontal_accuracy:.2f}m LatE {lat_err:.2f}m LonE {lon_err:.2f}m"

            if horizontal_accuracy is not None: payload['acc'] = horizontal_accuracy

        # Get altitude
        # gpslogger expects HAE by default unless MSL turned ON which can result in missing alt
        # See https://github.com/mendhak/gpslogger/issues/748
        if gps.isfinite(session.fix.altHAE):
            alt = session.fix.altHAE
            logString += f" alt: {alt}m"

            if alt is not None: payload['alt'] = alt
     
        # Get lat and lon
        if ((gps.isfinite(session.fix.latitude) and gps.isfinite(session.fix.longitude))):
            payload['lat'] = session.fix.latitude
            payload['lon'] = session.fix.longitude
            logString += " Lat %.6f Lon %.6f" % (session.fix.latitude, session.fix.longitude)
        else:
            logString += " Lat n/a Lon n/a"

        # Get timestamp of gpsd data
        if gps.TIME_SET & session.valid:
            timeStamp = gps.isotime(session.fix.time)

            payload['tst'] = timeStamp
                
            logString += ' Time: %s (%d)' % (session.fix.time, timeStamp)
        else:
            logString += ' Time: n/a'
   
        # Get speed
        if gps.isfinite(session.fix.speed):
            payload['vel'] = session.fix.speed # maybe * 3.6, # Convert m/s to km/h
      
        logString  += ' END'
        logging.debug(logString)

        return payload


    def isDataComplete(payload) -> bool:
        """
        Does the payload have all data required of gpslogger endpoint?

        Arguments:
            payload (dict): the potential gpslogger payload
        """
        required_keys = {"_type", "t", "batt", "bs", "tst", "lat", "lon", "vel", "acc", "alt"}
        return payload.keys() >= required_keys


    def checkData(self, payload) -> bool:
        """
        Does the payload meet all requirements for being enqueued for sampling then sending?

        Arguments:
            payload (dict): the potential gpslogger payload
        """
        if GpsdGateway.isDataComplete(payload) == False:
            return False
        if (self._lastTimestamp == int(payload['tst'])):
            logging.debug(f"Skipping payload with repeat timestamp {self._lastTimestamp}")
            return False
        return True


    def enqueueData(self, payload) -> None:
        """
        Enqueue a received gpsd payload for potential sampling and sending

        Arguments:
            payload (dict): the gpslogger payload
        """
        logging.debug(f"Enqueing payload {payload}")
        self._samplingQueue.put(payload)


    def sendData(self, id: int, url: string, headers, payload) -> bool:
        """
        Send a gpslogger payload to the specified url with http headers
        """
        success = False
        try:
            # gpslogger endpoint receives variables as json but validates auth via headers
            response = requests.post(url, json=payload, headers=headers, timeout=5)
            if response.status_code == 200:
                success = True
                logging.debug(f"Writer {id} sent to endpoint: {payload['lat']}, {payload['lon']}")
            elif response.status_code in (401, 403):
                logging.error(f"Writer {id} Authentication failed! Check your X-API-TOKEN. Status code: {response.status_code}")
            else:
                logging.error(f"Writer {id} Endpoint rejected data. Status code: {response.status_code}")
        except requests.exceptions.RequestException as req_err:
            logging.error(f"Writer {id} Network error forwarding to endpoint: {req_err}")

        return success

    #########################################################################
    # Thread entry methods
    #########################################################################

    def reader(self, id: int, stop_event, host: string, port: int) -> None:
        """
        Thread of id to continuously read data from gpsd on host:port and enqueue any valid payloads for sampling

        stop_event will signal thread to stop and return
        """
        logging.debug(f"Reader {id} starting for {host}:{port}")

        while not stop_event.is_set():
            session = None

            try:
                logging.info(f"Connecting to gpsd at tcp://{host}:{port}")
                session = gps.gps(host=host, port=port, mode=gps.WATCH_ENABLE) # TODO mode=gps.WATCH_ENABLE | gps.WATCH_NEWSTYLE
                logging.info(f"Connected to gpsd at: tcp://{host}:{port}")

                while (not stop_event.is_set()) and (0 == session.read()):
                    if not (gps.MODE_SET & session.valid):
                    # not useful, probably not a TPV message
                        continue
    
                    payload = GpsdGateway.processData(session)
                    if self.checkData(payload):
                        self._lastTimestamp = int(payload['tst'])
                        self.enqueueData(payload)

            # These exceptions just cause a reconnection attempt
            except ConnectionRefusedError as e:
                logging.error(f"Could not connect to gpsd at {host}:{port}: {e}")
                stop_event.wait(timeout=1) 
            except Exception as e:
                logging.error(f"Unknown error: {e}")
                stop_event.wait(timeout=1) 

        if session is not None: session.close()
        logging.debug(f"Reader {id} ending for {host}:{port}")


    def sampler(self, id: int, stop_event, interval: int) -> None:
        """
        Thread of id to perioidically at interval seconds to sample 1 payload from the samplingQueue and enqueue it on sendingQueue for sending to gpslogger endpoint

        stop_event will signal thread to stop and return
        """
        logging.debug(f"Sampler {id} starting")

        latest = None

        while not stop_event.is_set():
            count = 0
            while not stop_event.is_set():
                try:
                    latest = self._samplingQueue.get_nowait()
                    if latest is not None: count += 1
                except queue.Empty:
                    break

            # if the queue is empty, then block for first entry to avoid adding latency (and either just started or already waited min time between sends)
            if latest == None:
                logging.debug(f"Sampler {id} Queue empty so do blocking get (count {count})")
                latest = self._samplingQueue.get(block=True)
                if latest is not None: count += 1

            if latest is not None:
                logging.debug(f"Sampler {id} Sampled 1 of {count} queued to send")
                self._sendQueue.put(latest)
                logging.debug(f"Sampler {id} Sleeping {interval}")
                stop_event.wait(timeout=interval) 
            else:
                logging.debug(f"Sampler {id} Queue empty after blocking get (count {count})")

        logging.debug(f"Sampler {id} ending")


    def writer(self, id: int, stop_event, url: string, headers) -> None:
        """
        Thread of id to continuously read payloads from sendingQueue and send payload to gpslogger endpoint at url with http headers

        stop_event will signal thread to stop and return
        """
        logging.debug(f"Writer {id} starting")

        latest = None

        while not stop_event.is_set():
            try:
                latest = self._sendQueue.get(block=True, timeout=1)
            except queue.Empty:
                continue

            if latest is not None:
                int_timestamp = int(time.time())
                queueSize = self._sendQueue.qsize()
                logging.debug(f"Writer {id} Sending payload (of {queueSize} @ {int_timestamp}): {latest})")
                if not self.sendData(id, url, headers, latest):
                    logging.error(f"Writer {id} Failed sending payload, re-enqueing @ {int_timestamp}: {latest})")
                    # MAYBE pause for a bit?
                    stop_event.wait(timeout=1) 
                    # if sending fails, toss it back on queue for later attempt?
                    self._sendQueue.put(latest)
        
        logging.debug(f"Writer {id} ending")
                
    #########################################################################
    # Main loop
    #########################################################################

    def run(self, server: str, port: int, url: str, token: str, interval: int, numwriters: int) -> None:
        """
        Run the gpsd-to-gpslogger-endpoint gateway

        Runs until unrecoverable error, KeyboardInterrupt, or kill signal
        """
        logging.info(f"Starting gpsd-to-gpslogger endpoint gateway...")
        logging.info(f"Targeting gpslogger endpoint URL: {url}")

        # Set up the authorization header
        headers = {
            'Content-Type': 'application/json',
            'X-API-TOKEN': token
        }
     
        # stop signal event to threads, to make KeyboardInterrupt work
        stop_event = threading.Event()
        threads = []

        try:
            # Create reader thread
            t = threading.Thread(target=self.reader, args=(0, stop_event, server, port))
            readerThread = t
            threads.append(t)

            # Create sampler thread
            t = threading.Thread(target=self.sampler, args=(0, stop_event, interval))
            samplerThread = t
            threads.append(t)

            # Create writer threads
            writerThreads = []
            for id in range(numwriters):
                t = threading.Thread(target=self.writer, args=(id, stop_event, url, headers))
                writerThreads.append(t)
                threads.append(t)

            # run all threads
            for t in threads: t.start()

            # Keep the main thread alive and responsive to KeyboardInterrupt
            while any(t.is_alive() for t in threads):
               # A short timeout allows Ctrl+C to interrupt the sleep
               time.sleep(0.5)

        # These exceptions shut down the gateway
        except KeyboardInterrupt as e:
            logging.info(f"Shutting down gateway due to Control-C {e}")
        except GracefulExit as e:
            logging.info(f"Shutting down gateway due to kill signal: {e}")
        except Exception as e:
            logging.error(f"Unknown error: {e}")
        finally:
           logging.info(f"Signaling threads to stop and waiting...")
           # Signal all threads to drop out of their loops
           stop_event.set()

           # wait for all threads to join
           for t in threads: t.join()

           logging.info(f"Done")

#########################################################################
# Argument parsing
#########################################################################

def parse_arguments() -> configargparse.Namespace:
    """Parse arguments"""
    parser = configargparse.ArgumentParser(
        description="Configurable gateway to forward gpsd data to a GPSLogger-compatible endpoint"
    )
    parser.add_argument('-c', '--config', is_config_file=True, help='Path to config file')
    parser.add_argument('-s', '--server', default="localhost", help="hostname or IP address of the gpsd daemon (default: localhost).")
    parser.add_argument('-p', '--port', default="2947", help="port number of the gpsd daemon (default: 2947).")
    parser.add_argument('-u', '--url', required=True, help="GPSLogger endpoint URL (e.g., https://example.com).")
    parser.add_argument('-t', '--token', required=True, help="GPSLogger authorization token passed in the X-API-TOKEN header.")
    parser.add_argument('-i', '--interval', type=int, default=15, help="Interval time in seconds between endpoint updates (default: 15).")
    parser.add_argument('-w', '--numwriters', type=int, default=1, help="Number of writer threads to send to payloads to endpoint (default: 1)")
    parser.add_argument('-l', '--loglevel', default='WARNING', type=str.upper, choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], help='Set the logging level (default: WARNING)')
    return parser.parse_args()

#########################################################################
# Main
#########################################################################

def main():
    # Register the handler for the SIGTERM kill signal
    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGHUP, handle_sighup)

    args = parse_arguments()

    # Configure the logger
    logging.basicConfig(
        level=args.loglevel,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    app = GpsdGateway()
    app.run(
        server=args.server,
        port=args.port,
        url=args.url,
        token=args.token,
        interval=args.interval,
        numwriters=args.numwriters
    )

if __name__ == "__main__":
    main()
