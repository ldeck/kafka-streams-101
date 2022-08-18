# Kafka Streams 101 #

Following along with https://blog.rockthejvm.com/kafka-streams/

## WIP ##

This is a WIP following the above blog article.

## Setup ##

### Step 1: Start containers ###

    ./bin/start-containers

### Step 2: Init kafka topics ###

    ./bin/broker-shell
    create-topics
    exit


### Step 3: Run app ###

    sbt run

### Step 4: Produce events ###

    ./bin/broker-shell

    # add discount profiles
    kafka-console-producer --topic discounts --broker-list localhost:9092 --property parse.key=true --property key.separator=,
    profile1,{"profile":"profile1","amount":0.5 }
    profile2,{"profile":"profile2","amount":0.25 }
    profile3,{"profile":"profile3","amount":0.15 }
    Ctl-D

    # add users
    kafka-console-producer --topic discount-profiles-by-user --broker-list localhost:9092 --property parse.key=true --property key.separator=,
    Daniel,profile1
    Riccardo,profile2
    Ctl-D

    # add orders by user
    kafka-console-producer --topic orders-by-user --broker-list localhost:9092 --property parse.key=true --property key.separator=,
    Daniel,{"orderId":"order1","user":"Daniel","products":[ "iPhone 13","MacBook Pro 15"],"amount":4000.0 }
    Riccardo,{"orderId":"order2","user":"Riccardo","products":["iPhone 11"],"amount":800.0}
    Ctl-D

    # add payments
    kafka-console-producer --topic payments --broker-list localhost:9092 --property parse.key=true --property key.separator=,
    order1,{"orderId":"order1","status":"PAID"}
    order2,{"orderId":"order2","status":"PENDING"}
    Ctl-D

    exit

## Consumer ##

According to the above blog,

> If everything goes right, into the topic paid-orders we should find
> a message containing the paid order of the user "Daniel", containing an "iPhone 13" and a "MacBook Pro 15", and worth 2,000.0 Euro.

    ./bin/broker-shell
    kafka-console-consumer --bootstrap-server localhost:9092 --topic paid-orders --from-beginning
    exit

I.e., We should see the following event:

    {"orderId":"order1","user":"Daniel","products":["iPhone 13","MacBook Pro 15"],"amount":2000.0}

But I do not see it.

## Exit ##

If inside the broker shell, just type `exit`
