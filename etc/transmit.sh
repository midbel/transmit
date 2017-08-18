#!/bin/sh

### BEGIN INIT INFO
# Provides:          transmit
# Required-Start:    $network $syslog
# Required-Stop:     $network $syslog
# Default-Start:     2 3 4 5
# Default-Stop:
# Short-Description: transmit multicast packets from network to network
# Description:       transmit daemon to forward packets from mutlicast
#                    streams to another network missing connectivity to the
#                    former.
### END INIT INFO

PATH=/sbin:/bin:/usr/bin:/usr/sbin/usr/local/bin
NAME=transmit
DAEMON=/usr/local/bin/transmit
CONFIG=/usr/local/etc/transmit/relay.json
TRANSMIT_USER=root
TRANSMIT_GROUP=root
TRANSMIT_DIR=/var/run/$NAME
PIDFILE=$TRANSMIT_DIR/$NAME.pid


test -x $DAEMON || exit 5

. /lib/lsb/init-functions


case "$1" in
  start)
        log_daemon_msg "Starting daemon" "$NAME"
        if ! test -d $TRANSMIT_DIR; then
                mkdir -p $TRANSMIT_DIR
                chown -R $TRANSMIT_USER:$TRANSMIT_GROUP $TRANSMIT_DIR
        fi
        start-stop-daemon --start --name $NAME --make-pidfile --quiet --background --pidfile $PIDFILE --exec $DAEMON -- $CONFIG
        log_end_msg $?
    ;;
  stop)
        log_daemon_msg "Stopping daemon" "$NAME"
        start-stop-daemon --stop --pidfile $PIDFILE --name $NAME --retry 10
        log_end_msg $?
    ;;
  status)
        status_of_proc $DAEMON "transmit"
    ;;
  force-reload|restart)
    $0 stop
    $0 start
    ;;
  *)
    echo "Usage: $0 {start|stop|restart|force-reload}"
    exit 1
    ;;
esac

exit 0
