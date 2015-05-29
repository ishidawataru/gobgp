tool: log analysis with elasticsearch
===

1. install docker-compose

follow the instruction here https://docs.docker.com/compose/install/

2. startup logging components

type `$ docker-compose up`

3. run gobgp with syslog option

type `$ gobgpd --syslog="udp:localhost:514" -f gobgpd.conf -l debug`

4. go to http://<docker-host-ip>:5601

analyse gobgp log through kibana
