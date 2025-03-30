initialize-db:
	@echo "Initializing dbt project..."
	dbt clean
	dbt seed -t ${ENV}
	python3 seeds/abstraction_to_s3/load_aeroplane_models.py