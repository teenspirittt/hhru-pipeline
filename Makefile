default: run

run:
	docker-compose -f ./docker-airflow/docker-compose-LocalExecutor.yml up -d
	docker-compose -f ./docker-hadoop/docker-compose.yml up -d

stop:
	docker-compose -f ./docker-airflow/docker-compose-LocalExecutor.yml down
	docker-compose -f ./docker-hadoop/docker-compose.yml down

clean: stop
	docker-compose -f ./docker-airflow/docker-compose-LocalExecutor.yml down --volumes
	docker-compose -f ./docker-hadoop/docker-compose.yml down --volumes

restart: stop run
