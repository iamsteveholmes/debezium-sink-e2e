FROM gvenzl/oracle-xe:21.3.0-slim-faststart
#FROM gvenzl/oracle-free:slim-faststart

RUN mkdir -p /opt/oracle/oradata/recovery_area
RUN chgrp 54321 /opt/oracle/oradata
RUN chown 54321 /opt/oracle/oradata
RUN chgrp 54321 /opt/oracle/oradata/recovery_area
RUN chown 54321 /opt/oracle/oradata/recovery_area
