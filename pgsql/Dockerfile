# FILE: pgsql/Dockerfile
# PostgreSQL Docker image with CDC support

FROM postgres:15

# Set environment variables
ENV POSTGRES_USER=postgres
ENV POSTGRES_PASSWORD=postgres
ENV POSTGRES_DB=postgres

# Copy initialization script
COPY init.sql /docker-entrypoint-initdb.d/

# Copy PostgreSQL configuration
COPY postgresql.conf /etc/postgresql/postgresql.conf

# Expose PostgreSQL port
EXPOSE 5432

# Override the default command to use our config
CMD ["postgres", "-c", "config_file=/etc/postgresql/postgresql.conf"]