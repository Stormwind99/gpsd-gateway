FROM alpine:latest

LABEL org.opencontainers.image.title="gpsd to gpslogger endpoint gateway" \
      org.opencontainers.image.description="Configurable gateway to send gpsd TPV (Time Position Velocity) data to a GPSLogger-compatible endpoint" \
      org.opencontainers.image.version="1.0" \
      org.opencontainers.image.source="https://github.com/Stormwind99/gpsd-to-gpslogger-endpoint" \
      org.opencontainers.image.licenses="GPL-3.0-only"

# Install python requirements
RUN apk add --no-cache python3 py3-gpsd py3-requests py3-configargparse

# Create a non-root group and user to run as
RUN addgroup -S appgroup && adduser -S appuser -G appgroup

WORKDIR /app 
COPY . /app
RUN chown -R appuser:appgroup /app

USER appuser

# Exec form ensures signals (SIGTERM) pass directly to Python
ENTRYPOINT ["python3", "-u", "gpsd-to-gpslogger-endpoint.py"]

# CMD acts as default flags or can be overridden entirely
CMD []
