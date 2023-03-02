# Street congestion optimizer
Traffic congestion optimization with python, Apache Kafka and Apache Flink using a SUMO simulation of the italian city of Trento.

Requirements:

Download [Docker](https://www.docker.com/products/docker-desktop/ "Docker")   
Download [SUMO](https://www.eclipse.org/sumo/ "SUMO")

Set SUMO_HOME environment variable

How to use it:

    - Make sure that docker daemon is running
    - Change the path to SUMO_HOME in the 'sumo_starter.py' file
    - Open a shell and run 'python setup.py'

A SUMO gui will automatically open and you will be able to see cars moving inside the map and traffic lights changing color according to the algorithm
