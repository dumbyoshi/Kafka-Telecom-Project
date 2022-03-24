## installation guide
    $ docker-compose up -d
    
    for elk and kibana go to docker_elk dir and run below command
    
    $ docker-compose up -d

## upload connector(confluent connector 8083 port)
    copy dokcer-id of cnfldemos/cp-server-connect-datagen:0.5.0-6.2.0 container  like below
    05a5dd32da60   cnfldemos/cp-server-connect-datagen:0.5.0-6.2.0   "/etc/confluent/dock…"   About an hour ago   Up 2 minutes        0.0.0.0:8083->8083/tcp, 9092/tcp                 connect        

    $ docker cp .\connector\debezium-debezium-connector-mysql-1.7.1\ 05a5dd32da60:/usr/share/confluent-hub-components

    $ docker cp connector/confluentinc-kafka-connect-elasticsearch-11.1.7 05a5dd32da60:/usr/share/confluent-hub-components

    and restart docer container 
     $ docker-compose restart

## change your ip add in connector-json(both mysql and elk connector)  (ipconfig)
    192.168.48.1->yourIp

## create index in kibana(search kibana index in search bar)
    dbserver2.inventory.raw_telecom
