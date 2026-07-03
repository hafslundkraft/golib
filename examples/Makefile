.PHONY: lgtm-up run-kafkarator-processor-demo

# Starts the lgtm stack (Grafana + Tempo + Loki + Prometheus + OTel
# Collector) used by the demos below. Idempotent — no-ops if already
# running. Grafana UI: http://localhost:3000
lgtm-up:
	docker compose up -d lgtm

# Runs kafkarator_processor_demo with tracing exported to the lgtm stack
# instead of the console.
run-kafkarator-processor-demo: lgtm-up
	cd kafkarator_processor_demo && \
	OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318 \
	OTEL_RESOURCE_ATTRIBUTES=service.name=kafkarator-processor-demo,service.namespace=golib-examples \
	go run .
