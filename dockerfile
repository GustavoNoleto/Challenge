FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    build-essential gcc g++ ninja-build python3-dev \
    gnupg2 curl apt-transport-https \
    unixodbc-dev \
    && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update && ACCEPT_EULA=Y apt-get install -y msodbcsql17 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*


ENV DATABASE_DRIVER='ODBC Driver 17 for SQL Server'
ENV DATABASE_USER='cassio'
ENV DATABASE_PASSWORD='c4$$!0'
ENV DATABASE_HOST='10.12.66.4,49817'
ENV DATABASE_DATABASE='P12PRD'

ENV DATABASE_DRIVER_2='ODBC Driver 17 for SQL Server'
ENV DATABASE_USER_2='matias'
ENV DATABASE_PASSWORD_2='ctsMkNpc'
ENV DATABASE_HOST_2='192.168.0.5'
ENV DATABASE_DATABASE_2='rhlavronorte'

ENV LOGIN_USER='pbi'
ENV PASSWORD='dgE7eL9Rpcsm0EPBWk2c0pwXKWtwYd8d7YqkuB2YSOEX0J8Ux7Y67485f720458f'


WORKDIR /app

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "server.py"]
