TIMESTAMP=$(date +%s)

docker-clean:
	@docker ps -aq --filter name=xtkt_dev | xargs -r docker rm -f
	@docker rmi xtkt || true
	docker build -t xtkt --build-arg CACHEBUST=$TIMESTAMP .
	docker run -p 3000:8888 --name xtkt_dev xtkt
	docker ps -a