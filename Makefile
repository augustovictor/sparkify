run-db:
	docker run -it --rm -p 5432:5432 --name sparkify-db -e POSTGRES_DB=studentdb -e POSTGRES_USER=student -e POSTGRES_PASSWORD=student postgres:alpine

etl:
	python3 etl.py

autopep8:
	autopep8 --in-place --aggressive --aggressive *.py