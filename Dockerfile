FROM alpine:latest

LABEL org.opencontainers.image.title="gpsd gateway" \
      org.opencontainers.image.description="Configurable gateway to send gpsd TPV (Time Position Velocity) data to different endpoints" \
      org.opencontainers.image.source="https://github.com/Stormwind99/gpsd-gateway" \
      org.opencontainers.image.licenses="GPL-3.0-only"

# Removed org.opencontainers.image.version="1.1.0" expecting Github action to add it automatically

# Install python requirements
RUN apk add --no-cache python3 py3-gpsd py3-requests py3-configargparse py3-gpxpy

# Create a non-root group and user to run as
RUN addgroup -S appgroup && adduser -S appuser -G appgroup

WORKDIR /app 
COPY . /app
RUN chown -R appuser:appgroup /app

USER appuser

# Exec form ensures signals (SIGTERM) pass directly to Python
ENTRYPOINT ["python3", "-u", "gpsd-gateway.py"]

# CMD acts as default flags or can be overridden entirely
CMD []
