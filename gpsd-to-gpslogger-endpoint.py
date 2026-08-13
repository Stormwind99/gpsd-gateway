#! /usr/bin/env python3
"""
gpsd to gpslogger endpoint gateway
        
Configurable gateway to send gpsd TPV (Time Position Velocity) data to a GPSLogger-compatible endpoint

Threading model:
* main thread starts all other threads up, handles shutdown signal processing and on demand tells threads to stop, waits for threads to complete, then returns
* stats thread that receives stats from all threads and periodically logs those stats if enabled
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
import math
import json
from datetime import datetime


#########################################################################
# Misc
#########################################################################

# create a namespaced logger for the module
# so if module is reused, the future user can configure logging for just this module
logger = logging.getLogger(__name__)


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
    logger.info("SIGHUP received - nothing to do")

from collections import defaultdict
from datetime import datetime


#########################################################################
# Stats tracking
#########################################################################

class Stats:
    """
    Track stats via key and value, in a thread-safe manner, both total and since last report
    """

    def __init__(self, interval_seconds=0):
        self._lock = threading.Lock()

        self._totalStats = defaultdict(int)
        self._recentStats = defaultdict(int)

        self._totalDateTime = datetime.now()
        self._recentDateTime = self._totalDateTime

    def increment(self, metric_name, value=1):
        """Thread-safe increment of a counter."""
        with self._lock:
            self._recentStats[metric_name] += value
            self._totalStats[metric_name] += value

    def set(self, metric_name, value=0):
        """Thread-safe update of a specific value."""
        with self._lock:
            self._recentStats[metric_name] = value
            self._totalStats[metric_name] = value

    def pptd(td: timedelta) -> str:
        """Pretty print a timedelta in a compact day hours minutes seconds format"""

        # Extract days
        days = td.days

        # Extract hours, minutes, and seconds from total seconds
        total_seconds = int(td.total_seconds())
        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)

        # Adjust hours if you want them wrapped per day (0-23)
        hours_per_day = hours % 24

        dstr = ""

        if days != 0:
            dstr = f"{days}d{hours_per_day:d}h{minutes:02d}m{seconds:02d}s"
        elif hours_per_day != 0:
            dstr = f"{hours_per_day:d}h{minutes:02d}m{seconds:02d}s"
        elif minutes != 0:
            dstr = f"{minutes:d}m{seconds:02d}s"
        else:
            dstr = f"{seconds:d}s"

        return dstr

    def report(self):
        """Logs the total and recent stats and resets the recent stats"""
        with self._lock:
            # Create a shallow copy to log without holding the lock too long
            recentStatsSnapshot = self._recentStats.copy()
            totalStatsSnapshot = self._totalStats.copy()
            # calculate time diffs
            nowDateTime = datetime.now()
            recentTimeDiff = nowDateTime - self._recentDateTime
            totalTimeDiff = nowDateTime - self._totalDateTime
            # clear recent stats
            self._recentStats.clear()
            self._recentDateTime = nowDateTime

        logger.info(f"Total stats last {Stats.pptd(totalTimeDiff)}: {json.dumps(totalStatsSnapshot, sort_keys=True)}")
        logger.info(f"Recent stats last {Stats.pptd(recentTimeDiff)}: {json.dumps(recentStatsSnapshot, sort_keys=True)}")


#########################################################################
# GpsdGateway
#########################################################################

class GpsdGateway:
    """
    Gateway to read GPS data from gpsd and forward it to gpslogger style endpoint
    """

    # type hint shortcuts
    type HeaderDict = dict[str, str]
    type PayloadValue = str | int | float
    type PayloadDict = dict[str, PayloadValue]

    def __init__(self):
        self._lastTimestamp: int = 0
        """int: Unix timestamp of last data from gpsd session that was enqueued for sampling and potential sending"""
        self._samplingQueue: queue.Queue = queue.Queue()
        """The queue that all fully-valid-for-sending gpsd readings are enqueued to for sampling (only send 1 every X seconds)"""
        self._sendQueue: queue.Queue = queue.Queue()
        """The queue containing gpslogger payloads to send"""
        self._stats = Stats()
        """Stats: stats for this gateway"""


    #########################################################################
    # Misc
    #########################################################################

    def checkStatsReport(self, interval: int = 0) -> None:
        """Report stats if interval != 0"""
        if (interval != 0):
            self._stats.report()

    def handleSighup(self, signum, frame) -> None:
        logger.info("SIGHUP received - forcing stats report")
        self._stats.report()


    #########################################################################
    # Data processing
    #########################################################################

    def processData(session: gps.gps.gps) -> PayloadDict:
        """
        Given a gpsd session object, process it and return an equivalent gpslogger-endpoint-compatible payload

        Arguments:
            session (gps.gps.gps): gpsd library session

        Returns:
            PayloadDict: gpslogger format payload
        """
        logString = 'GPSd sent Mode: %s(%d)' % (("Invalid", "NO_FIX", "2D", "3D")[session.fix.mode], session.fix.mode)

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
            horizontal_accuracy = math.hypot(lat_err, lon_err)
            if horizontal_accuracy is not None: payload['acc'] = horizontal_accuracy
            logString += f" Acc: {horizontal_accuracy:.2f}m LatE {lat_err:.2f}m LonE {lon_err:.2f}m"

        # Get altitude
        # gpslogger expects HAE by default unless MSL turned ON which can result in missing alt
        # See https://github.com/mendhak/gpslogger/issues/748
        if gps.isfinite(session.fix.altHAE):
            alt = session.fix.altHAE
            if alt is not None: payload['alt'] = alt
            logString += f" alt: {alt}m"
     
        # Get lat and lon
        if ((gps.isfinite(session.fix.latitude) and gps.isfinite(session.fix.longitude))):
            payload['lat'] = session.fix.latitude
            payload['lon'] = session.fix.longitude
            logString += " Lat %.6f Lon %.6f" % (session.fix.latitude, session.fix.longitude)

        # Get timestamp of gpsd data
        if gps.TIME_SET & session.valid:
            timeStamp = gps.isotime(session.fix.time)
            payload['tst'] = int(timeStamp)
            logString += ' Time: %s (%d)' % (session.fix.time, timeStamp)
   
        # Get speed
        if gps.isfinite(session.fix.speed):
            payload['vel'] = session.fix.speed # maybe * 3.6, # Convert m/s to km/h
      
        logString  += ' END'
        logger.debug(logString)

        return payload


    def isDataComplete(payload: PayloadDict) -> bool:
        """
        Does the payload have all data required of gpslogger endpoint?

        Arguments:
            payload (PayloadDict): the potential gpslogger payload

        Returns:
            bool: if payload is complete enough to be accepted by gpslogger endpoint
        """
        required_keys = {"_type", "t", "batt", "bs", "tst", "lat", "lon", "vel", "acc", "alt"}
        return payload.keys() >= required_keys


    def checkData(self, payload: PayloadDict) -> bool:
        """
        Does the payload meet all requirements for being enqueued for sampling then sending?

        Arguments:
            payload (PayloadDict): the potential gpslogger payload

        Returns:
            bool: should payload be queued for sending to gpslogger endpoint
        """
        if GpsdGateway.isDataComplete(payload) == False:
            self._stats.increment("payloadsIncomplete")
            return False

        if (self._lastTimestamp == int(payload['tst'])):
            logger.debug(f"Skipped enqueing payload with repeat timestamp {self._lastTimestamp}")
            self._stats.increment("payloadsRepeated")
            return False
        return True


    def enqueueData(self, payload: PayloadDict) -> None:
        """
        Enqueue a received gpsd payload for potential sampling and sending

        Arguments:
            payload (PayloadDict): the gpslogger payload
        """
        logger.debug(f"Enqued payload {payload}")
        self._samplingQueue.put(payload)
        self._stats.increment("payloadsEnqueued")


    def sendData(self, id: int, url: string, headers: HeaderDict, payload: PayloadDict) -> bool:
        """
        Send a gpslogger payload to the specified url with http headers

        Arguments:
            id (int): thread id number
            url (string): URL of gpslogger endpoint to send data to
            headers (HeaderDict): http headers to use in gpslogger http post
            payload (PayloadDict): gpslogger format payload of TPV data

        Returns:
            bool: was payload sent to gpslogger endpoint
        """
        success = False
        try:
            # gpslogger endpoint receives variables as json but validates auth via headers
            response = requests.post(url, json=payload, headers=headers, timeout=5)
            if response.status_code == 200:
                success = True
                logger.debug(f"Writer {id} Sent to endpoint: {payload['lat']}, {payload['lon']}")
            elif response.status_code in (401, 403):
                logger.error(f"Writer {id} Authentication failed! Check your X-API-TOKEN. Status code: {response.status_code}")
            else:
                logger.error(f"Writer {id} Endpoint rejected data. Status code: {response.status_code}")
        except requests.exceptions.RequestException as req_err:
            logger.error(f"Writer {id} Network error forwarding to endpoint: {req_err}")

        return success

    #########################################################################
    # Thread entry methods
    #########################################################################

    def stats(self, id: int, stop_event: threading.Event, interval: int) -> None:
        logger.debug(f"Stats {id} starting")

        # even if interval is 0 to disable stats, go ahead and keep thread around (future reconfig on sighup)
        sleepinterval = interval if interval != 0 else None

        while not stop_event.is_set():
            interrupted = stop_event.wait(timeout=sleepinterval) 
            if (not interrupted):
                self.checkStatsReport(interval)

        logger.debug(f"Stats {id} ending")


    def reader(self, id: int, stop_event: threading.Event, server: string, port: int) -> None:
        """
        Thread of id to continuously read data from gpsd on server:port and enqueue any valid payloads for sampling

        Arguments:
            id (int): thread id number
            stop_event (threading.Event): use to signal thread to stop and return
            server (string): hostname of gpsd server
            port (int): TCP port of gpsd server
        """
        logger.debug(f"Reader {id} starting for {server}:{port}")

        while not stop_event.is_set():
            session = None

            try:
                logger.info(f"Connecting to gpsd at tcp://{server}:{port}")
                session = gps.gps(host=server, port=port, mode=gps.WATCH_ENABLE) # TODO mode=gps.WATCH_ENABLE | gps.WATCH_NEWSTYLE
                logger.info(f"Connected to gpsd at: tcp://{server}:{port}")
                self._stats.increment("gpsdConnects")

                while (not stop_event.is_set()) and (0 == session.read()):
                    if not (gps.MODE_SET & session.valid):
                        # not useful, probably not a TPV message
                        self._stats.increment("gpsdMessagesNA")
                        continue
   
                    payload = GpsdGateway.processData(session)
                    if self.checkData(payload):
                        self._lastTimestamp = int(payload['tst'])
                        self.enqueueData(payload)

            # These exceptions just cause a reconnection attempt
            except ConnectionRefusedError as e:
                logger.error(f"Could not connect to gpsd at {server}:{port}: {e}")
                self._stats.increment("gpsdConnectFailed")
                stop_event.wait(timeout=1) 
            except Exception as e:
                logger.error(f"Unknown error: {e}")
                self._stats.increment("gpsdOtherErrors")
                stop_event.wait(timeout=1) 

        if session is not None: session.close()
        logger.debug(f"Reader {id} ending for {server}:{port}")


    def sampler(self, id: int, stop_event: threading.Event, interval: int) -> None:
        """
        Thread of id to perioidically at interval seconds to sample 1 payload from the samplingQueue and enqueue it on sendingQueue for sending to gpslogger endpoint

        Arguments:
            id (int): thread id number
            stop_event (threading.Event): use to signal thread to stop and return
            interval (int): seconds between sampling 1 payload from the samplingQueue
        """
        logger.debug(f"Sampler {id} starting")

        while not stop_event.is_set():
            latest = None
            count = 0

            while not stop_event.is_set():
                try:
                    latest = self._samplingQueue.get_nowait()
                    if latest is not None: count += 1
                    # interval of 0 means send every enqueued payload, so skip taking more than 1 payload from queue
                    if interval == 0: break
                except queue.Empty:
                    break

            # if the queue is empty, then block for first entry to avoid adding latency (and either just started or already waited min time between sends)
            if latest is None:
                logger.debug(f"Sampler {id} Queue empty so do blocking get (count {count})")
                while not stop_event.is_set():
                    try:
                        latest = self._samplingQueue.get(block=True, timeout=1)
                        if latest is not None:
                            count += 1
                            break
                    except queue.Empty:
                        pass

            if latest is not None:
                self._sendQueue.put(latest)
                logger.debug(f"Sampler {id} Sampled 1 of {count} and queued to send, and sleeping for {interval}")
                self._stats.increment("samplerCount")
                self._stats.increment("samplerEvaled", count)
                stop_event.wait(timeout=interval) 
            else:
                logger.debug(f"Sampler {id} Queue empty after blocking get (count {count})")

        logger.debug(f"Sampler {id} ending")


    def writer(self, id: int, stop_event: threading.Event, url: string, headers: HeaderDict) -> None:
        """
        Thread of id to continuously read payloads from sendingQueue and send payload to gpslogger endpoint at url with http headers

        Arguments:
            id (int): thread id number
            stop_event (threading.Event): use to signal thread to stop and return
            url (string): gpslogger endpoint URL to http POST payload
            headers (HeaderDict): http headers to apply to POST
        """
        logger.debug(f"Writer {id} starting")

        latest = None

        while not stop_event.is_set():
            try:
                latest = self._sendQueue.get(block=True, timeout=1)
            except queue.Empty:
                continue

            if latest is not None:
                int_timestamp = int(time.time())
                queueSize = self._sendQueue.qsize()
                logger.debug(f"Writer {id} Sending payload (of {queueSize} @ {int_timestamp}): {latest})")
                if not self.sendData(id, url, headers, latest):
                    self._stats.increment("sendsFailed")
                    logger.error(f"Writer {id} Failed sending payload, re-enqueing @ {int_timestamp}: {latest})")
                    # pause for a bit?
                    stop_event.wait(timeout=1) 
                    # if sending fails, toss it back on queue for later attempt?
                    # TODO: stop queue from eating all memory if endpoint down for a long time
                    self._sendQueue.put(latest)
                else:
                    self._stats.increment("sendsSuccess")
        
        logger.debug(f"Writer {id} ending")


    #########################################################################
    # Main loop
    #########################################################################

    def run(self, server: str, port: int, url: str, token: str, interval: int, numwriters: int, statsinterval: int) -> None:
        """
        Run the gpsd-to-gpslogger-endpoint gateway

        Runs until unrecoverable error, KeyboardInterrupt, or kill signal

        Arguments:
            server (string): hostname of gpsd server
            port (int): TCP port of gpsd server
            url (string): gpslogger endpoint URL to http POST payload
            token (string): X-API-TOKEN for auth when POSTing to gpslogger endpoint URL
            interval (int): seconds between sampling 1 payload from the samplingQueue
            numwriters (int): number of writer threads to gpslogger endpoing to start
            statsinterval (int): number of seconds between stats reports, or 0 for none
        """
        logger.info(f"Starting gpsd-to-gpslogger endpoint gateway...")
        logger.info(f"Targeting gpslogger endpoint URL: {url}")

        # Set up the authorization header
        headers = {
            'Content-Type': 'application/json',
            'X-API-TOKEN': token
        }
     
        # stop signal event to threads, to make KeyboardInterrupt and graceful SIGKILL work
        stop_event = threading.Event()

        # all threads being created and run
        threads = []

        try:
            # Create stats thread
            t = threading.Thread(target=self.stats, args=(0, stop_event, statsinterval))
            statsThread = t
            threads.append(t)

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
            logger.info(f"Shutting down due to Control-C {e}")
        except GracefulExit as e:
            logger.info(f"Shutting down due to kill signal: {e}")
        except Exception as e:
            logger.error(f"Unknown error: {e}")
        finally:
            logger.info(f"Signaling threads to stop and waiting...")

            # Signal all threads to drop out of their loops
            stop_event.set()

            # wait for main threads to join, but keep stats thread running
            for t in threads: t.join()

            # output one last stats report
            self.checkStatsReport(statsinterval)
 
            logger.info(f"Done")
 

#########################################################################
# Argument parsing
#########################################################################

def parse_arguments() -> configargparse.Namespace:
    """
    Parse arguments

    Returns:
        configargparse.Namespace: parsed configuration
    """

    parser = configargparse.ArgumentParser(
        description="Configurable gateway to forward gpsd data to a GPSLogger-compatible endpoint"
    )
    parser.add_argument('-c', '--config', is_config_file=True, help='Path to config file')
    parser.add_argument('-s', '--server', default="localhost", help="hostname or IP address of the gpsd daemon (default: localhost).")
    parser.add_argument('-p', '--port', type=int, default=2947, help="port number of the gpsd daemon (default: 2947).")
    parser.add_argument('-u', '--url', required=True, help="GPSLogger endpoint URL (e.g., https://example.com).")
    parser.add_argument('-t', '--token', required=True, help="GPSLogger authorization token passed in the X-API-TOKEN header.")
    parser.add_argument('-i', '--interval', type=int, default=15, help="Interval time in seconds between endpoint updates, 0 sends every payload (default: 15).")
    parser.add_argument('-w', '--numwriters', type=int, default=1, help="Number of writer threads to send to payloads to endpoint (default: 1)")
    parser.add_argument('-l', '--loglevel', default='WARNING', type=str.upper, choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], help='Set the logging level (default: WARNING)')
    parser.add_argument('-r', '--statsinterval', type=int, default=0, help="Interval time in seconds between stats reports to INFO log, 0 disables, SIGHUP forces report (default: 0).")
    return parser.parse_args()


#########################################################################
# Main
#########################################################################

def main():
    # Register signal handlers
    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGHUP, handle_sighup)

    args = parse_arguments()

    # Configure the logger
    logging.basicConfig(
        level=args.loglevel,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    app = GpsdGateway()
    # switch SIGHUP to be handled by GpsdGateway object for forced stats reporting
    signal.signal(signal.SIGHUP, app.handleSighup)
    app.run(
        server=args.server,
        port=args.port,
        url=args.url,
        token=args.token,
        interval=args.interval,
        numwriters=args.numwriters,
        statsinterval=args.statsinterval
    )

if __name__ == "__main__":
    main()
