About the solution:
-------------------
I chose to make a RESTful HTTP sever in Flask which uses Elasticsearch as its db to achieve sub-second
performance. For that I modeled the data to be structured in a fashion that allow that.
Finally, I dockerized the server and created a docker-compose file that sets up the containers
and runs the solution.

Content overview:
-----------------
1. data_preprations folder: contains the logic and data outputs for modeling and preparing
the data, making it ready to be uploaded to Elasticsearch for serving.
2. lib folder: contains the logic for the HTTP server and db population with the modeled data.
In addition it containes the modeled data csv files that are the outcome of the previous.
3. Dockerfile which defines the image of section 2
4. docker-compose.yml file that launches the image of the HTTP server and in addition sets
up a container with Elasticsearch cluster.

How to run:
----------
Run: docker-compose up
and wait until the HTTP server prints that its up.
NOTICE:
1. Initialization takes about 8-10 minutes (for populating the ES db).
Until population is done, the server is down.
2. Elasticsearch have memory requirements that must be satisfied for it to work.
    - Make sure that the docker VM has at least 2.5MB of memory 
    - Allow it to map the virtual memory it needs by running on your docker host
    the following cmd: sudo sysctl vm.max_map_count=262144
3. HTTP server listens on port 5000, make sure you have the docker-machine ip

Once the service is up, here are examples of GET requests:
http://localhost:5000/avg-age?start=2016-01-01%2012:47:10&end=2016-01-20%2013:00:52&device_id=1&content_id=6
http://localhost:5000/viewer-count?start=2016-01-01%2012:47:10&end=2016-01-20%2013:00:52&device_id=1&content_id=6
http://localhost:5000/gender-dist?start=2016-01-01%2012:47:10&end=2016-01-20%2013:00:52&device_id=1&content_id=6
